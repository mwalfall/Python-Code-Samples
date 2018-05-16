import os
import sys
import luigi
import datetime as dt
import pandas as pd
import numpy as np

from luigi.contrib.bigquery import *

from Connection import Connection
from Pipeline import Pipeline
from Messages import slack_msg
from LoadAdminTablesBQVars import LoadAdminTablesBQVars as var
from DataCleanser import DataCleanser
from BaseTasks import RunPipelineLogTask
from SQLServerTarget import PipelineTasks


class CleanUp:
    """
    The class deletes the CSV files that are used during processing.
    """

    def __init__(self):
        return

    def sites(self):
        csv_list = [var.SITES_BQ_FULLPATH, var.SITES_ANALYTICS_FULLPATH,
                    var.SITES_UPDATE_FULLPATH, var.SITES_NEW_FULLPATH]
        self.remove(csv_list)

    def advertisers(self):
        csv_list = [var.ADVERTISER_BQ_FULLPATH, var.ADVERTISER_ANALYTICS_FULLPATH,
                    var.ADVERTISER_UPDATE_FULLPATH, var.ADVERTISER_NEW_FULLPATH]
        self.remove(csv_list)

    def campaigns(self):
        csv_list = [var.CAMPAIGN_BQ_FULLPATH, var.CAMPAIGN_ANALYTICS_FULLPATH,
                    var.CAMPAIGN_UPDATE_FULLPATH, var.CAMPAIGN_NEW_FULLPATH]
        self.remove(csv_list)

    def activities(self):
        csv_list = [var.ACTIVITY_BQ_FULLPATH, var.ACTIVITY_ANALYTICS_FULLPATH,
                    var.ACTIVITY_UPDATE_FULLPATH, var.ACTIVITY_NEW_FULLPATH]
        self.remove(csv_list)

    def remove(self, csv_list):
        for csv in csv_list:
            try:
                os.remove(csv)
            except OSError:
                pass
        return


class AdvertiserGroupETL(luigi.Task):
    """
    This ETL facilitates the management of Advertiser Groups. It pulls any Advertiser Group from
    BQ that does not have an advertiser_group_id set to NULL. The results are stored in the
    [Admin].[AdvertiserGroups] table.

    Every Advertiser belongs to an Advertiser Group. The AdvertiserGroup table has a field named
    IsDisplayed the states if Advertisers belonging to the specified group should be displayed in
    Metrics.

    If it is decided that a group is to be displayed the IsDisplayed flag is to be set to True.
    The default setting for a new Advertiser Group is IsDisplayed False. The Product manager must
    explicitly state that the group is to be displayed in Metrics.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "AdvertiserGroupETL")
    conn = Connection()
    cleanup = CleanUp()

    met_tar_sa = conn.sa_create_engine(conn.met_src_cstr)
    met_tar_cur = conn.met_tar.return_cursor()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def run(self):
        # Tuple containing bq and metrics data frames.
        extracted_data = self.extract()

        # Tuple containing new groups and updated groups data frames.
        transformed_data = self.transform(extracted_data)

        # Returns True of updates successful.
        is_successful = self.load(transformed_data)

        if is_successful:
            self.process_completed()
            self.pl.run_once_insert(self.run_date)

    def extract(self):
        """
        Runs queries that extract groups from BQ Advertisers data and Analytics.
        :return: Tuple containing BQ and Analytics Advertiser Group data.
        """
        bq_data = self.get_groups_from_bq()
        metrics_data = self.get_groups_from_analytics()

        return bq_data, metrics_data

    def get_groups_from_bq(self):
        """
        Obtain Advertiser Group data from BQ.
        """
        groups_from_bq = pd.io.gbq.read_gbq(query=var.GET_ADVERTISER_GROUPS_FROM_BQ.format(var.DATASET_ID),
                                            project_id=var.PROJECT_ID,
                                            private_key=var.BQ_PRIVATE_KEY,
                                            dialect='standard',
                                            index_col=None)
        return groups_from_bq

    def get_groups_from_analytics(self):
        """
        Obtain Advertiser Group data from Analytics.
        """
        met_src_con = self.conn.met_src.return_conn()
        return pd.read_sql(var.GET_ADVERTISER_GROUPS_FROM_ANALYTICS, con=met_src_con)

    def transform(self, extracted_data):

        # Tidy up.
        groups_from_bq = extracted_data[0]
        groups_from_bq.columns = var.GET_BQ_ADVERTISER_GROUP_COLUMN_NAMES
        groups_from_bq['advertiser_group_id'] = pd.to_numeric(groups_from_bq['advertiser_group_id'])

        groups_from_analytics = extracted_data[1]
        groups_from_analytics.columns = var.GET_ANALYTICS_ADVERTISER_GROUP_COLUMN_NAMES
        groups_from_analytics['ID'] = pd.to_numeric(groups_from_analytics['ID'])

        merged_results = pd.merge(left=groups_from_bq,
                                  right=groups_from_analytics,
                                  how='left',
                                  left_on='advertiser_group_id',
                                  right_on='ID')

        new_groups = self.process_new_groups(merged_results)
        updated_groups = self.process_updated_groups(merged_results)

        return new_groups, updated_groups

    def process_new_groups(self, merged_results):

        new_records = merged_results[merged_results['ID'].isnull()]
        newbies = new_records.drop(new_records.columns[[2, 3]], axis=1)
        newbies['IsDisplayed'] = pd.Series(0, index=newbies.index)

        # Tidy up data.
        newbies.columns = var.GET_ANALYTICS_ADVERTISER_GROUP_COLUMN_NAMES
        newbies['ID'] = newbies['ID'].astype(int)

        return newbies

    def process_updated_groups(self, merged_results):
        """
        Extract the Advertiser Groups that are to be updated.
        """
        existing_records = merged_results[merged_results['ID'].notnull()]
        updated_records = existing_records[(existing_records['advertiser_group'] != existing_records['Name'])]

        updates = updated_records.drop(updated_records.columns[[1,3]], axis=1)

        # Tidy up data.
        updates.columns = ['Name', 'ID']
        updates['ID'] = updates['ID'].astype(int)

        return updates

    def load(self, transformed_data):
        """
        Load the transformed data into the Analytics database.
        """
        new_groups = transformed_data[0]
        updated_groups = transformed_data[1]

        try:
            if len(updated_groups) > 0:
                for index, row in updated_groups.iterrows():
                    update = var.UPDATE_ADVERTISER_GROUPS.format(str(row['Name']).replace("'", "''"), int(row['ID']))
                    self.met_tar_cur.execute(update).commit()

            if len(new_groups) > 0:
                new_groups.to_sql(name='AdvertiserGroups',
                                  con=self.met_tar_sa,
                                  if_exists='append',
                                  index=False,
                                  schema='Admin')

        except Exception as e:
            self.cleanup.advertiser_groups(self.run_date.date())
            msg = 'ERROR occurred while loading Advertiser Groups: {}'.format(e)
            self.pl.logger.error(msg)
            print(msg)
            return False

        return True

    def process_completed(self):
        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())
        self.met_tar_cur.close()


class LoadAdminBQRelationshipValidation:
    """
    This class is used to ensure referential integrity between the Advertisers,
    Campaigns and Activities table.
    """

    def __init__(self):
        self.conn = Connection()
        self.met_src_con = self.conn.met_src.return_conn()
        self.pl = Pipeline("LoadAdminTables", "LoadAdminBQRelationshipValidation")

    def validate_advertiser_ids(self, result_set, table_name):
        """
        Determine if the all the advertiser ids transformed from the match table are
        in the Admin.Advertisers table.
        """

        advertiser_ids = self.get_advertiser_ids()

        for item in result_set['AdvertisersID']:
            result = np.where(advertiser_ids['ID'] == item)
            x = result[0]
            if len(x) == 0:
                msg = 'WARNING: BQ {} table contains Advertiser IDs that'.format(table_name)
                msg = '{} are not in the Admin.Advertisers table. Value: {}.'.format(msg, item)
                self.pl.logger.error(msg)

    def validate_campaign_ids(self, result_set, table_name):
        """
        Determine if the all the advertiser ids transformed from the match table are
        in the Admin.Advertisers table.
        """

        campaign_ids = self.get_campaign_ids()

        for item in result_set['CampaignID']:
            result = np.where(campaign_ids['ID'] == item)
            x = result[0]
            if len(x) == 0:
                msg = 'WARNING: BQ {} table contains Advertiser IDs that are not'.format(table_name)
                msg = '{} in the Admin.Advertisers table. Value: {}.'.format(msg, item)
                self.pl.logger.error(msg)

    def get_advertiser_ids(self):
        return pd.read_sql(var.GET_ADVERTISER_IDS, con=self.met_src_con)

    def get_campaign_ids(self):
        return pd.read_sql(var.GET_CAMPAIGN_IDS, con=self.met_src_con)


class ExtractCampaigns(luigi.Task):
    """
    Extracts Campaign data from BigQuery and the Metrics database.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "ExtractCampaigns")
    conn = Connection()
    data_cleanser = DataCleanser()
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def run(self, data_set=''):

        try:
            if data_set == '':
                data_set = var.DATASET_ID

            # Ensure there is a connection to BQ and the Campaigns table exists.
            self.is_campaigns_table_present(data_set)

            # Get campaigns from BQ.
            campaigns_from_bq = self.get_campaigns_from_bq(data_set)
            if len(campaigns_from_bq) == 0:
                return

            # Tidy up the data.
            campaigns_from_bq.columns = var.GET_BQ_CAMPAIGN_COLUMN_NAMES
            campaigns_from_bq['campaign_id'] = pd.to_numeric(campaigns_from_bq['campaign_id'])
            campaigns_from_bq['advertiser_id'] = pd.to_numeric(campaigns_from_bq['advertiser_id'])

            campaigns_from_bq['campaign'] = campaigns_from_bq['campaign'].str.replace('"', '')
            campaigns_from_bq['campaign'] = campaigns_from_bq['campaign'].str.replace('|', '+')
            campaigns_from_bq['campaign'] = campaigns_from_bq['campaign'].str.replace('\\r', '')
            campaigns_from_bq['campaign'] = campaigns_from_bq['campaign'].str.replace('\\r\\n', '')

            # Save to csv.
            campaigns_from_bq.to_csv(var.CAMPAIGN_BQ_FULLPATH, sep='|', index=False, encoding='utf-16')

            # Get the campaigns for Analytics.
            campaigns_from_analytics = self.get_campaigns_from_analytics()

            # Tidy up data.
            campaigns_from_analytics.columns = var.GET_ANALYTICS_CAMPAIGN_COLUMN_NAMES
            campaigns_from_analytics['OrganizationID'].fillna(0).round(0).astype(int)

            # Save to csv.
            campaigns_from_analytics.to_csv(var.CAMPAIGN_ANALYTICS_FULLPATH, sep='|', index=False, encoding='utf-16')

        except Exception as e:
            self.cleanup.campaigns()
            msg = 'ERROR while attempting to extract Campaigns data: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())

    def is_campaigns_table_present(self, data_set):
        """
        Ensures that the Campaigns table exists in BigQuery.
        :param data_set: Dataset where the table resides.
        :return: True if table exists. Otherwise, throws an exception that halts processing.
        """
        exists = pd.io.gbq.read_gbq(query=var.DETERMINE_IF_CAMPAIGN_EXISTS_QRY.format(data_set),
                                    project_id=var.PROJECT_ID,
                                    private_key=var.BQ_PRIVATE_KEY,
                                    dialect='standard',
                                    index_col=None)

        if len(exists) == 0:
            raise Exception("PROCESS HALTED: ExtractCampaigns unable to "
                            "locate BQ 'match_table_campaigns_4789' table.")

        return True

    def get_campaigns_from_bq(self, data_set):
        """
        Get the campaign data from BQ.
        :param data_set: The GCP dataset where the data resides.
        :return: Campaign data obtained from BQ table.
        """
        campaigns_from_bq = pd.io.gbq.read_gbq(query=var.GET_CAMPAIGNS_FROM_BQ.format(data_set),
                                               project_id=var.PROJECT_ID,
                                               private_key=var.BQ_PRIVATE_KEY,
                                               dialect='standard',
                                               index_col=None)
        return campaigns_from_bq

    def get_campaigns_from_analytics(self):
        """
        Obtains the Campaign data that exists in the Metrics database.
        :return: Analytics Campaign data.
        """
        met_src_con = self.conn.met_src.return_conn()
        return pd.read_sql(var.GET_CAMPAIGNS_FROM_ANALYTICS, con=met_src_con)


class TransformCampaigns(luigi.Task):
    """
    This class transforms the campaigns data obtained from the ExtractCampaigns class so that it
    can be saved to the  Analytics.Campaigns table.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "TransformCampaign")
    conn = Connection()
    validator = LoadAdminBQRelationshipValidation()
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return ExtractCampaigns()

    def run(self):

        try:
            campaigns_from_bq = pd.read_csv(var.CAMPAIGN_BQ_FULLPATH, sep='|', encoding='utf-16')
            campaigns_from_analytics = pd.read_csv(var.CAMPAIGN_ANALYTICS_FULLPATH, sep='|', encoding='utf-16')

            self.validate_bq_campaign_data(campaigns_from_bq)

            merged_results = pd.merge(left=campaigns_from_bq,
                                      right=campaigns_from_analytics,
                                      how='left',
                                      left_on='campaign_id',
                                      right_on='ID')

            self.process_new_campaigns(merged_results)

            self.process_campaign_updates(merged_results)

        except Exception as e:
            self.cleanup.campaigns()
            msg = 'ERROR occurred while transforming campaigns data: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())

    def process_new_campaigns(self, merged_results):
        """
        Extract the new campaigns.
        """

        new_records = merged_results[merged_results['ID'].isnull()]
        newbies = new_records.drop(new_records.columns[[5, 6, 7, 8, 9, 10]], axis=1)

        # Tidy up data.
        newbies.columns = var.GET_ANALYTICS_CAMPAIGN_COLUMN_NAMES
        newbies['ID'] = newbies['ID'].astype(int)
        newbies['AdvertisersID'] = newbies['AdvertisersID'].astype(int)

        # Ensure referential integrity with existing metrics advertisers.
        self.validator.validate_advertiser_ids(newbies, 'match_table_campaigns')

        # Save the new campaigns in a csv file.
        newbies.to_csv(var.CAMPAIGN_NEW_FULLPATH, sep='|', index=False, encoding='utf-16')

    def process_campaign_updates(self, merged_results):
        """
        Extract the Campaigns that are to be updated.
        """

        existing_records = merged_results[merged_results['ID'].notnull()]
        existing_records['r_start_date'] = pd.to_numeric(existing_records['CampaignStartDate'].str.replace('-', ''))
        existing_records['r_end_date'] = pd.to_numeric(existing_records['CampaignEndDate'].str.replace('-', ''))
        updated_records = existing_records[(existing_records['Campaign'] != existing_records['campaign']) |
                                           (existing_records['r_start_date'] != existing_records['campaign_start_date']) |
                                           (existing_records['r_end_date'] != existing_records['campaign_end_date'])]

        updates = updated_records.drop(updated_records.columns[[5, 6, 7, 8, 9, 11, 12]], axis=1)

        # Tidy up data.
        updates.columns = var.GET_CAMPAIGN_UPDATE_COLUMNS
        updates['OrganizationID'] = updates['OrganizationID'].fillna(0).round(0).astype(int)
        updates['ID'] = updates['ID'].astype(int)
        updates['AdvertisersID'] = updates['AdvertisersID'].astype(int)

        # Ensure referential integrity with existing analytics advertisers.
        self.validator.validate_advertiser_ids(updates, 'match_table_campaigns')

        # Save the updates to the advertisers in a csv file.
        updates.to_csv(var.CAMPAIGN_UPDATE_FULLPATH, sep='|', index=False, encoding='utf-16')

    def validate_bq_campaign_data(self, campaigns_from_bq):
        """ Validate the dataset based on the Campaigns EDA. """

        no_of_campaign_records = len(campaigns_from_bq)

        # Advertiser ids should be numeric.
        if not np.issubdtype(campaigns_from_bq['advertiser_id'].dtype, np.number):
            msg = 'PROCESS HALTED: '
            msg = 'match_table_campaigns contains Advertiser IDs contains records that are not numeric.'.format(msg)
            raise Exception(msg)

        # Advertiser id should be greater than zero.
        if (campaigns_from_bq['advertiser_id'] < 1).any():
            msg = 'PROCESS HALTED: '
            msg = 'match_table_campaigns contains Advertiser IDs that are less zero or a negative number.'.format(msg)
            raise Exception(msg)

        # Campaign ID should be numeric.
        if not np.issubdtype(campaigns_from_bq['campaign_id'].dtype, np.number):
            msg = 'PROCESS HALTED: match_table_campaigns'
            msg = ' {} contains Campaign IDs contains records that are not numeric.'.format(msg)
            raise Exception(msg)

        # Campaign id should be greater than zero.
        if (campaigns_from_bq['campaign_id'] < 1).any():
            msg = 'PROCESS HALTED: match_table_campaigns contains campaign id'
            msg = ' {} that is less zero or a negative number.'.format(msg)
            raise Exception(msg)

        # Every record should have a campaign name.
        if campaigns_from_bq['campaign'].count() != no_of_campaign_records:
            raise Exception('PROCESS HALTED: Some records in match_table_campaign do not contain a campaign name.')

        # Truncate campaign names that are greater than 120 characters.
        campaigns_from_bq['campaign'] = campaigns_from_bq['campaign'].apply(lambda x: x[:120])

        # Every record should have a campaign start date.
        if campaigns_from_bq['campaign_start_date'].count() != no_of_campaign_records:
            msg = 'PROCESS HALTED: Some records in match_table_campaign'
            msg = ' {} do not contain a campaign start date.'.format(msg)
            raise Exception(msg)

        # Every record should have a campaign end date.
        if campaigns_from_bq['campaign_end_date'].count() != no_of_campaign_records:
            msg = 'PROCESS HALTED: Some records in match_table_campaign'
            msg = ' {} do not contain a campaign end date.'.format(msg)
            raise Exception(msg)


class LoadCampaigns(luigi.Task):
    """
    This class persists the Campaigns data to the Analytics.Campaigns table.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "LoadCampaigns")
    conn = Connection()
    met_tar_cur = conn.met_tar.return_cursor()
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return TransformCampaigns()

    def run(self):
        try:
            # Update the Advertisers.
            activity_updates = pd.read_csv(var.CAMPAIGN_UPDATE_FULLPATH, sep='|', encoding='utf-16')

            for index, row in activity_updates.iterrows():
                update = var.UPDATE_CAMPAIGN.format(str(row['Campaign']).replace("'", "''"),
                                                    row['CampaignStartDate'],
                                                    row['CampaignEndDate'],
                                                    int(row['OrganizationID']),
                                                    int(row['OrganizationID']),
                                                    int(row['AdvertisersID']),
                                                    int(row['ID']))
                self.met_tar_cur.execute(update).commit()

            # Save the new Advertisers.
            self.met_tar_cur.execute(var.CAMPAIGN_BULK_INSERT.format(var.RMT_CAMPAIGN_NEW_FULLPATH)).commit()

        except Exception as e:
            self.cleanup.campaigns()
            msg = 'ERROR occurred while loading campaigns: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.cleanup.campaigns()
        self.process_completed()

    def process_completed(self):
        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())
        self.met_tar_cur.close()


class ExtractActivities(luigi.Task):
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "ExtractActivities")
    conn = Connection()
    data_cleanser = DataCleanser()
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def run(self, data_set=''):
        if data_set == '':
            data_set = var.DATASET_ID

        try:
            self.is_activities_table_present(data_set)
            self.is_activity_cats_table_present(data_set)

            activities_from_bq = self.get_activities_from_bq(data_set)
            if len(activities_from_bq) == 0:
                return

            # Exit if there are no records to process.
            if len(activities_from_bq) == 0:
                return

            # Tidy up the data.
            activities_from_bq.columns = var.GET_ACTIVITIES_COLUMN_NAMES_FROM_BQ
            activities_from_bq = self.data_cleanser.transform_string_columns_bulk(activities_from_bq)

            # Save to csv.
            activities_from_bq.to_csv(var.ACTIVITY_BQ_FULLPATH, sep='|', index=False, encoding='utf-16')

            # Get the advertisers for Metrics.
            activities_from_analytics = self.get_activities_from_analytics()
            activities_from_analytics.columns = var.GET_ACTIVITIES_COLUMN_NAMES_FROM_ANALYTICS

            # Save to csv.
            activities_from_analytics.to_csv(var.ACTIVITY_ANALYTICS_FULLPATH, sep='|', index=False, encoding='utf-16')

        except Exception as e:
            self.cleanup.activities()
            msg = 'ERROR while attempting to extract Activities data: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())

    def is_activities_table_present(self, data_set):
        exists = pd.io.gbq.read_gbq(query=var.DETERMINE_IF_ACTIVITIES_EXISTS_QRY.format(data_set),
                                    project_id=var.PROJECT_ID,
                                    private_key=var.BQ_PRIVATE_KEY,
                                    dialect='standard',
                                    index_col=None)
        if len(exists) != 0:
            raise Exception('PROCESS HALTED: ExtractActivities unable to locate BQ TABLE_ACTIVITIES table.')

        return True

    def is_activity_cats_table_present(self, data_set):
        exists = pd.io.gbq.read_gbq(query=var.DETERMINE_IF_ACTIVITY_CATS_EXISTS_QRY.format(data_set),
                                    project_id=var.PROJECT_ID,
                                    private_key=var.BQ_PRIVATE_KEY,
                                    dialect='standard',
                                    index_col=None)
        if exists.ix[0, 0] != 1:
            raise Exception('PROCESS HALTED: ExtractActivities unable to locate BQ TABLE_ACTIVITY_CATS table.')

        return True

    def get_activities_from_bq(self, data_set):
        activities_from_bq = pd.io.gbq.read_gbq(query=var.GET_ACTIVITIES_FROM_BQ.format(data_set, data_set),
                                                project_id=var.PROJECT_ID,
                                                private_key=var.BQ_PRIVATE_KEY,
                                                dialect='standard',
                                                index_col=None)
        return activities_from_bq

    def get_activities_from_analytics(self):
        met_src_con = self.conn.met_src.return_conn()
        return pd.read_sql(var.SELECT_ACTIVITIES_FROM_ANALYTICS, con=met_src_con)


class TransformActivities(luigi.Task):
    """
    This class transforms the obtained Activities data into a format that complies with
    the Analytics.Activities table.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "TransformActivities")
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return ExtractActivities()

    def run(self):

        try:
            activities_from_bq = pd.read_csv(var.ACTIVITY_BQ_FULLPATH, sep='|', encoding='utf-16')
            activities_from_analytics = pd.read_csv(var.ACTIVITY_ANALYTICS_FULLPATH, sep='|', encoding='utf-16')

            merged_results = pd.merge(left=activities_from_bq,
                                      right=activities_from_analytics,
                                      how='left',
                                      left_on=['ac_activity_id', 'a_advertiser_id', 'a_campaign_id'],
                                      right_on=['ID', 'AdvertisersID', 'CampaignsID'])

            self.process_new_activities(merged_results)
            self.process_updated_activities(merged_results)

        except Exception as e:
            self.cleanup.activities()
            msg = 'ERROR while attempting to transform Activities data: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())

    def process_new_activities(self, merged_results):
        """
        Extract the new advertisers.
        """

        new_records = merged_results[merged_results['ID'].isnull()]
        newbies = new_records.drop(new_records.columns[[4, 5, 6, 7]], axis=1)

        # Tidy up data.
        newbies.columns = ['ID', 'Activity', 'AdvertisersID', 'CampaignsID']
        newbies['ID'] = newbies['ID'].astype(int)
        newbies['AdvertisersID'] = newbies['AdvertisersID'].astype(int)
        newbies['CampaignsID'] = newbies['CampaignsID'].astype(int)

        # Save the new advertisers in a csv file.
        newbies.to_csv(var.ACTIVITY_NEW_FULLPATH, sep='|', index=False, encoding='utf-16')

    def process_updated_activities(self, merged_results):
        """
        Extract the Advertisers that are to be updated.
        """

        existing_records = merged_results[merged_results['ID'].notnull()]
        updated_records = existing_records[existing_records['Activity'] != existing_records['ac_activity']]
        updates = updated_records.drop(updated_records.columns[[0, 2, 3, 6]], axis=1)

        # Tidy up data.
        updates.columns = ['Activity', 'ID', 'AdvertisersID', 'CampaignsID']
        updates['ID'] = updates['ID'].astype(int)
        updates['AdvertisersID'] = updates['AdvertisersID'].astype(int)
        updates['CampaignsID'] = updates['CampaignsID'].astype(int)

        # Save the updates to the Advertisers in a csv file.
        updates.to_csv(var.ACTIVITY_UPDATE_FULLPATH, sep='|', index=False, encoding='utf-8')


class LoadActivities(luigi.Task):
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "LoadActivities")
    conn = Connection()
    met_tar_cur = conn.met_tar.return_cursor()
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return TransformActivities()

    def run(self):
        try:
            # Update the Advertisers.
            activity_updates = pd.read_csv(var.ACTIVITY_UPDATE_FULLPATH, sep='|')
            for index, row in activity_updates.iterrows():
                activity = str(row['Activity']).replace("'", "''")
                update = var.UPDATE_ACTIVITY.format(activity,
                                                    int(row['ID']),
                                                    int(row['CampaignsID']),
                                                    int(row['AdvertisersID']))
                self.met_tar_cur.execute(update).commit()

            # Save the new Advertisers.
            self.met_tar_cur.execute(var.ACTIVITY_BULK_INSERT.format(var.RMT_ACTIVITY_NEW_FULLPATH)).commit()

        except Exception as e:
            self.cleanup.activities()
            msg = 'ERROR while attempting to transform Activities data: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.cleanup.activities()
        self.process_completed()

    def process_completed(self):
        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())
        self.met_tar_cur.close()


class ExtractSites(luigi.Task):
    """
    This class obtains site data from BigQuery and Analytics.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "ExtractAdvertisers")
    conn = Connection()
    data_cleanser = DataCleanser()
    cleanup = CleanUp()

    met_src_con = conn.met_src.return_conn()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def run(self, data_set=''):

        try:
            if data_set == '':
                data_set = var.DATASET_ID

            self.is_sites_table_present(data_set)

            # Get sites from BQ.
            sites_from_bq = self.get_sites_from_bq(data_set)
            if len(sites_from_bq) == 0:
                return

            # Tidy up bq data.
            sites_from_bq.columns = var.GET_SITES_BQ_COLUMN_NAMES

            sites_from_bq['Site_DCM'] = sites_from_bq['Site_DCM'].str.replace('"', '')
            sites_from_bq['Site_DCM'] = sites_from_bq['Site_DCM'].str.replace('|', '+')
            sites_from_bq['Site_DCM'] = sites_from_bq['Site_DCM'].str.replace('\\r', '')
            sites_from_bq['Site_DCM'] = sites_from_bq['Site_DCM'].str.replace('\\r\\n', '')

            # Ensure site_id_dcm field contains numeric data.
            sites_from_bq = self.validate_bq_data(sites_from_bq)
            sites_from_bq['Site_ID_DCM'] = sites_from_bq['Site_ID_DCM'].astype(int)

            # Persist to csv.
            sites_from_bq.to_csv(var.SITES_BQ_FULLPATH, sep='|', index=False, encoding='utf-16')

            # Get sites from Analytics.
            sites_from_analytics = self.get_sites_from_analytics()

            # Tidy up analytic data.
            sites_from_analytics.columns = var.GET_SITES_ANALYTICS_COLUMN_NAMES
            sites_from_analytics['ID'].astype(int)

            # Persist to csv.
            sites_from_analytics.to_csv(var.SITES_ANALYTICS_FULLPATH, sep='|', index=False, encoding='utf-16')

        except Exception as e:
            self.cleanup.sites()
            msg = 'ERROR while attempting to extract Sites data: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())

    def is_sites_table_present(self, data_set):
        """
        Ensures that BQ table containing sites is present.
        """

        exists = pd.io.gbq.read_gbq(query=var.DETERMINE_IF_SITES_EXISTS_QRY.format(data_set),
                                    project_id=var.PROJECT_ID,
                                    private_key=var.BQ_PRIVATE_KEY,
                                    dialect='standard',
                                    index_col=None)
        if len(exists) == 0:
            self.cleanup.sites()
            raise Exception("PROCESS HALTED: ExtractSites unable "
                            "to locate BQ 'match_table_sites_4789' table.")

        return True

    def get_sites_from_bq(self, data_set):
        """
        Obtains sites data from BQ.
        """

        sites_from_bq = pd.io.gbq.read_gbq(query=var.GET_SITES_FROM_BQ.format(data_set),
                                           project_id=var.PROJECT_ID,
                                           private_key=var.BQ_PRIVATE_KEY,
                                           dialect='standard',
                                           index_col=None)
        return sites_from_bq

    def get_sites_from_analytics(self):
        """
        Obtains site data from Analytics.
        """
        sites_pd = pd.read_sql(var.GET_SITES_FROM_ANALYTICS, con=self.met_src_con)
        return sites_pd

    def validate_bq_data(self, sites_from_bq):
        """
        Ensure that all Site_ID_DCM values are numeric.
        """
        sites_from_bq[sites_from_bq.Site_ID_DCM.apply(lambda x: x.isnumeric())].set_index('Site_ID_DCM')
        return sites_from_bq


class TransformSites(luigi.Task):
    """
    This class transforms the Sites data into a from that can be used by
    the Analytics database.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "TransformSites")
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return ExtractSites()

    def run(self):

        try:
            sites_from_bq = pd.read_csv(var.SITES_BQ_FULLPATH, sep='|', encoding='utf-16')
            sites_from_analytics = pd.read_csv(var.SITES_ANALYTICS_FULLPATH, sep='|', encoding='utf-16')

            merged_results = pd.merge(left=sites_from_bq,
                                      right=sites_from_analytics,
                                      how='left',
                                      left_on='Site_ID_DCM',
                                      right_on='ID')

            self.process_new_sites(merged_results)
            self.process_site_updates(merged_results)

        except Exception as e:
            self.cleanup.sites()
            msg = 'ERROR occurred in TransformSites: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())

    def process_new_sites(self, merged_results):
        """
        This methods uses the merged data to determine new sites in the BQ data.
        """

        new_records = merged_results[merged_results['ID'].isnull()]
        newbies = new_records.drop(new_records.columns[[2, 3]], axis=1)

        # Tidy up the data.
        newbies.columns = ['ID', 'Site']
        newbies['ID'] = newbies['ID'].astype(int)

        # Save the new sites in a csv file.
        newbies.to_csv(var.SITES_NEW_FULLPATH, sep='|', index=False, encoding='utf-16')

    def process_site_updates(self, merged_results):
        """
        This methods uses the merged data to determine updated sites in the BQ data.
        """

        existing_records = merged_results[merged_results['ID'].notnull()]
        updated_records = existing_records[existing_records['SiteName'] != existing_records['Site_DCM']]
        updates = updated_records.drop(updated_records.columns[[2, 3]], axis=1)

        # Tidy up the data.
        updates.columns = ['ID', 'SiteName']
        updates['ID'] = updates['ID'].astype(int)

        # Save the updates to csv file.
        updates.to_csv(var.SITES_UPDATE_FULLPATH, sep='|', index=False, encoding='utf-16')


class LoadSites(luigi.Task):
    """
    The class persists the transformed sites data to the Metrics database.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "LoadSites")
    conn = Connection()
    met_tar_cur = conn.met_tar.return_cursor()
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return TransformSites()

    def run(self):
        try:
            # Persist updated Sites.
            sites_updates = pd.read_csv(var.SITES_UPDATE_FULLPATH, sep='|', encoding='utf-16')
            for index, row in sites_updates.iterrows():
                site_name = str(row['SiteName']).replace("'", "''")
                update = var.UPDATE_SITE.format(site_name, int(row['ID']))
                self.met_tar_cur.execute(update).commit()

            # Persist new Sites.
            self.met_tar_cur.execute(var.SITES_BULK_INSERT.format(var.RMT_SITES_NEW_FULLPATH)).commit()

        except Exception as e:
            self.cleanup.sites()
            msg = 'ERROR while attempting to persist Sites data: {}'.format(e)
            self.pl.logger.error(msg)
            self.met_tar_cur.close()
            sys.exit(msg)

        self.cleanup.sites()
        self.process_completed()

    def process_completed(self):
        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())
        self.met_tar_cur.close()


class ExtractAdvertisers(luigi.Task):
    """
    Extracts Advertiser data from BigQuery tables.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "ExtractAdvertisers")
    conn = Connection()
    data_cleanser = DataCleanser()
    cleanup = CleanUp()

    met_src_cur = conn.met_src.return_cursor()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return AdvertiserGroupETL()

    def run(self, data_set=''):
        if data_set == '':
            data_set = var.DATASET_ID

        self.is_advertisers_table_present(data_set)

        # Process Advertisers from BQ.
        advertiser_group_ids = self.get_advertiser_groups_str()
        advertisers_from_bq = self.get_advertisers_from_bq(data_set, advertiser_group_ids)
        if len(advertisers_from_bq) == 0:
            return

        advertisers_from_bq.columns = ['n_id', 'n_Advertiser', 'n_AdvertiserGroup', 'n_AdvertiserGroup_id']
        advertisers_from_bq = self.data_cleanser.transform_string_columns_bulk(advertisers_from_bq)
        advertisers_from_bq.to_csv(var.ADVERTISER_BQ_FULLPATH, sep='|', index=False, encoding='utf-16')

        # Process Advertisers from Analytics.
        advertisers_from_analytics = self.get_advertisers_from_analytics()
        advertisers_from_analytics.columns = ['adm_id',
                                              'AdminAdvertiser',
                                              'AdminAdvertiserGroup',
                                              'AdminAdvertiserGroupID']
        advertisers_from_analytics.to_csv(var.ADVERTISER_ANALYTICS_FULLPATH, sep='|', index=False, encoding='utf-16')

        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())
        self.met_src_cur.close()

    def is_advertisers_table_present(self, data_set):
        exists = pd.io.gbq.read_gbq(query=var.DETERMINE_IF_ADVERTISERS_EXISTS_QRY.format(data_set),
                                    project_id=var.PROJECT_ID,
                                    private_key=var.BQ_PRIVATE_KEY,
                                    dialect='standard',
                                    index_col=None)
        if len(exists) == 0:
            raise Exception("PROCESS HALTED: ExtractAdvertisers unable "
                            "to locate BQ 'table_advertisers' table.")

        return True

    def get_advertiser_groups_str(self):
        try:
            ad_groups_tuple = self.met_src_cur.execute(var.GET_ADVERTISER_GROUPS_FROM_ANALYTICS).fetchall()

        except Exception as e:
            error_msg = var.GET_ADVERTISER_GROUPS_FROM_ANALYTICS_ERROR_MSG.format(e)
            self.pl.logger.error(error_msg)
            sys.exit(error_msg)

        group_id_str = ''
        for group in ad_groups_tuple:
            group_id_str = group_id_str + "'{}',".format(group[0])

        # Remove the last comma.
        group_id_str = group_id_str[0:len(group_id_str)-1]

        return group_id_str

    def get_advertisers_from_bq(self, data_set, advertiser_ids):
        advertisers_from_bq = pd.io.gbq.read_gbq(query=var.GET_ADVERTISERS_QRY.format(data_set, advertiser_ids),
                                                 project_id=var.PROJECT_ID,
                                                 private_key=var.BQ_PRIVATE_KEY,
                                                 dialect='standard',
                                                 index_col=None)
        return advertisers_from_bq

    def get_advertisers_from_metrics(self):
        met_src_con = self.conn.met_src.return_conn()
        advertisers_pd = pd.read_sql(var.SELECT_ADVERTISERS_FROM_ANALYTICS, con=met_src_con)
        return advertisers_pd


class TransformAdvertisers(luigi.Task):
    """
    This class transforms the advertisers data into a from that can be used by
    the Analytics database.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "TransformAdvertisers")
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return ExtractAdvertisers()

    def run(self):

        try:
            advertisers_from_bq = pd.read_csv(var.ADVERTISER_BQ_FULLPATH, sep='|', encoding='utf-16')
            advertisers_from_analytics = pd.read_csv(var.ADVERTISER_ANALYTICS_FULLPATH, sep='|', encoding='utf-16')

            merged_results = pd.merge(left=advertisers_from_bq,
                                      right=advertisers_from_analytics,
                                      how='left',
                                      left_on='n_id',
                                      right_on='adm_id')

            self.process_new_advertisers(merged_results)
            self.process_advertiser_updates(merged_results)

        except Exception as e:
            self.cleanup.advertisers()
            msg = 'ERROR while transforming Advertisers data: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.pl.tasks_insert(self.load_date, self.run_date)

    def process_new_advertisers(self, merged_results):
        """
        Obtain the new advertisers from merged data.
        """

        new_records = merged_results[merged_results['adm_id'].isnull()]
        newbies = new_records.drop(new_records.columns[[4, 5, 6, 7]], axis=1)

        # Tidy up data.
        newbies.columns = var.GET_ADVERTISER_COLUMNS
        newbies['ID'] = newbies['ID'].astype(int)

        # Save the new advertisers in a csv file.
        newbies.to_csv(var.ADVERTISER_NEW_FULLPATH, sep='|', index=False, encoding='utf-16')

    def process_advertiser_updates(self, merged_results):
        """
        Obtain the Advertisers that are to be updated.
        """

        existing_records = merged_results[merged_results['adm_id'].notnull()]
        updated_records = existing_records[
            (existing_records['n_Advertiser'] != existing_records['AdminAdvertiser']) |
            (existing_records['n_AdvertiserGroup'] != existing_records['AdminAdvertiserGroup']) |
            (existing_records['n_AdvertiserGroup_id'] != existing_records['AdminAdvertiserGroupID'])]

        updates = updated_records.drop(updated_records.columns[[4, 5, 6, 7]], axis=1)

        # Tidy up the data.
        updates.columns = var.GET_ADVERTISER_COLUMNS
        updates['ID'] = updates['ID'].astype(int)

        # Save the updates to the advertisers in a csv file.
        updates.to_csv(var.ADVERTISER_UPDATE_FULLPATH, sep='|', index=False, encoding='utf-16')


class LoadAdvertisers(luigi.Task):
    """
    The class persists Advertiser data to the Advertisers database.
    """
    run_date = luigi.DateSecondParameter(default=dt.datetime.today() - dt.timedelta(days=1))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("LoadAdminTablesBQ", "LoadAdvertisers")
    conn = Connection()
    met_tar_cur = conn.met_tar.return_cursor()
    cleanup = CleanUp()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def requires(self):
        return TransformAdvertisers()

    def run(self):

        try:
            # Persist updated Sites.
            advertiser_updates = pd.read_csv(var.ADVERTISER_UPDATE_FULLPATH, sep='|', encoding='utf-16')
            for index, row in advertiser_updates.iterrows():
                advertiser_name = str(row['Advertiser']).replace("'", "''")
                advertiser_group_name = str(row['AdvertiserGroup']).replace("'", "''")

                update = var.UPDATE_ADVERTISER.format(advertiser_name,
                                                      advertiser_group_name,
                                                      int(row['AdvertiserGroupID']),
                                                      int(row['ID']))
                self.met_tar_cur.execute(update).commit()

            # Persist new Sites.
            self.met_tar_cur.execute(var.SITES_BULK_INSERT.format(var.RMT_ADVERTISER_NEW_FULLPATH)).commit()

        except Exception as e:
            self.cleanup.advertisers()
            msg = 'ERROR occurred while loading advertisers into database: {}'.format(e)
            self.pl.logger.error(msg)
            sys.exit(msg)

        self.cleanup.advertisers()
        self.process_completed()

    def process_completed(self):
        self.pl.tasks_insert(self.load_date.date(), self.run_date.date())
        self.met_tar_cur.close()


class RunAdminTasks(RunPipelineLogTask):
    run_date = dt.datetime.today() - dt.timedelta(days=1)
    load_date = dt.datetime.today()
    start_time = dt.datetime.today()
    pl = Pipeline("LoadAdminTablesBQ", "RunAdminTasks")

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.run_date.date())

    def run(self):

        try:
            yield [LoadSites(run_date=self.run_date, load_date=self.load_date),
                   LoadAdvertisers(run_date=self.run_date, load_date=self.load_date),
                   LoadCampaigns(run_date=self.run_date, load_date=self.load_date),
                   LoadActivities(run_date=self.run_date, load_date=self.load_date)]

            self.pl.log_insert(self.load_date, self.start_time)
        except Exception as e:
            sys.exit(e)

        self.process_completed()

    def process_completed(self):
        """
        Create the control record that informs Luigi that processing completed successfully.
        """
        self.pl.tasks_insert(self.load_date, self.run_date)


if __name__ == "__main__":
    lu = luigi.run()
    if lu is not True:
        slack_msg("LoadAdminTables", "The Admin Tables ETL failed.")
