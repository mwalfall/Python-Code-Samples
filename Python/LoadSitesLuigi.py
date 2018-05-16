import datetime as dt
import json
import luigi
import os
import pandas as pd
import sys
import urllib
import urllib.error
import urllib.response

from Pipeline import Pipeline
from Messages import slack_msg
from Connection import Connection
from LoadMonitoringVars import LoadMonitoringVars as var
from Configuration import CheckStatus
from SQLServerTarget import PipelineTasks


class SiteManager:
    """
    Maintains the active sites that exist in the application for use in DW and Tableau workbook creation.
    """
    def __init__(self):
        self.conn = Connection()
        self.pw_src_conn = self.conn.pw_src.return_conn()
        self.pw_src_cur = self.conn.pw_src.return_cursor()
        self.pw_tar_cur = self.conn.pw_tar.return_cursor()
        self.pl = Pipeline("LoadSites", "SiteManager")

    def get_hash(self, url):
        """
        Sites are stored as hashcodes within the DW. This method seeks to obtain a hashcode for a
        particular site url. If one is not found a hash is created.
        :param url: site URL
        :return: Hashcode for that site.
        """
        # Look for the url in the DimSites table.
        result = self.get_url_hash_dim_sites(url)
        if len(result) > 0:
            return result

        # Look for the hash in previous fact entries.
        result = self.get_hash_from_previous_facts(url)
        if len(result) > 0:
            return result

        # URL was not found. Create a new entry.
        self.create_new_dim_entry(url)

        # Return that entry.
        result = self.get_url_hash_dim_sites(url)
        if len(result) > 0:
            return result

        # CatchAll.
        return None

    def get_url_hash_dim_sites(self, url):
        """
        Get hashcode for previously stored site.
        :param url: Site URL
        :return: Hashcode
        """
        try:
            dims = self.pw_src_cur.execute(var.SELECT_HASH_FROM_DIM.format(url)).fetchall()

        except Exception as e:
            error_msg = "An error occurred what attempting to get hashcode from DimSites. MESSAGE: {}"\
                .format(e)
            self.pl.logger.error(error_msg)
            sys.exit(error_msg)

        # Extract hashcode from query result.
        if len(dims) > 0:
            for dim in dims:
                return dim[0]

        return None

    def get_hash_from_previous_facts(self, url):
        """
        Extract hashcode from previously stored facts. Included for backwards compatibility.
        :param url: Site URL
        :return: Hashcode for that site.
        """
        try:
            facts = self.pw_src_cur.execute(var.SELECT_HASH_FROM_FACT.format(url)).fetchall()

        except Exception as e:
            error_msg = 'An error occurred while getting hashcode from DimSites. MESSAGE: {}'.format(e)
            self.pl.logger.error(error_msg)
            sys.exit(error_msg)

        # Extract hashcode from query result.
        if len(facts) > 0:
            for fact in facts:
                return fact[0]

        return ''

    def create_new_dim_entry(self, url):
        """
        Creates a new entry in the DimSites bale with the resulting hashcode for the URL.
        :param url: Site URL
        :return: True if record was successfully saved. Otherwise, processing halts.
        """
        use_default = -1
        try:
            self.pw_tar_cur.execute(var.INSERT_SITE.format(use_default, use_default, use_default, url)).commit()
        except Exception as e:
            error_msg = "Unable to delete from DimSites table. ResultIDs: {}. ERROR Message: {}"\
                .format(url, e)
            self.pl.logger.exception(error_msg)
            sys.exit(error_msg)

        return True


class CleanUpTempFiles:
    """
    The class deletes the CSV files that are used during processing.
    """
    def remove(self):

        for the_file in os.listdir(var.DATA_SITES_PTH):
            temp_file = os.path.join(var.DATA_SITES_PTH, the_file)
            try:
                os.remove(temp_file)
            except OSError:
                pass
        return


class ExtractAPICASitesPerformance:
    """
    Extract Site performance data.
    """
    def __init__(self):
        self.pl = Pipeline("LoadSites", "ExtractSitesPerformance")

    def run(self, json_dict_summary_result, start_date):
        """
        This method acts as a controller for the process that obtains the aggregate performance
        data for the sites.
        :param json_dict_summary_result: The result file tht was obtained by ExtractSitesSummary.
        :param start_date: run date and time used to uniquely name json file that is used to persist results.
        """

        performance_json_list = []

        result_ids = self.get_result_ids(json_dict_summary_result)
        check_id = self.get_check_id(json_dict_summary_result)

        for result_id in result_ids:
            url = self.get_url(result_id, check_id)
            performance_json_list.append(self.get_data(url))

        full_path = self.get_file_path(start_date, check_id)
        self.save_date(full_path, performance_json_list)

    def get_result_ids(self, json_dict_summary_result):
        result_ids = []

        if json_dict_summary_result is None or len(json_dict_summary_result) == 0:
            return ()

        for summary in json_dict_summary_result:
            result_ids.append(summary['identifier'])

        return result_ids

    def get_check_id(self, json_dict_summary_result):
        return json_dict_summary_result[0]['check_id']

    def get_url(self, result_id, check_id):
        """
        Build the URL to obtain the aggregates for a specified time span.
        """
        url = var.BUILD_URL.format(var.BASE_URL, check_id, result_id, var.AUTH_TICKET)
        return url

    def get_data(self, url):
        """
        Use the built URL to obtain data.
        :param url: API call.
        :return: Performance data.
        """
        try:
            response = urllib.request.urlopen(url)
        except Exception as e:
            error_msg = "FAILED to obtain data from URL. ERROR MESSAGE: {} --URL: {} ".format(e, url)
            self.pl.logger.exception(error_msg)
            sys.exit(e)

        # The HTTP call returns a JSON array of monitors.
        json_list_result = response.read()
        json_dict_result = json.loads(json_list_result.decode('utf-8'))

        if not json_dict_result:
            self.pl.logger.info("No data to process.")
            return None

        return json_dict_result

    def get_file_path(self, start_date, check_id):
        prefix = var.SITES_PERFORMANCE.format(check_id)
        filename = get_filename(start_date, prefix, 'json')
        return os.path.join(var.DATA_SITES_PTH, filename)

    def save_date(self, full_path, json_dict_result):
        with open(full_path, 'w') as outfile:
            json.dump(json_dict_result, outfile)


class ExtractSitesSummary(luigi.Task):
    """
    Extracts Site Summary data.
    """
    start_date = luigi.DateSecondParameter(default=(dt.datetime.today() - dt.timedelta(days=1)))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    extract_performance = ExtractSitesPerformance()
    tmp_files = CleanUpTempFiles()
    pl = Pipeline("LoadSites", "ExtractSitesSummary")

    # This class ensures that the status of the APICA Checks are up-to-date.
    check_status = CheckStatus()

    # Database connections
    conn = Connection()
    pw_src_cur = conn.pw_src.return_cursor()
    pw_tar_cur = conn.pw_tar.return_cursor()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.start_date.date())

    def run(self):
        processing_time_span = self.get_processing_timespan(self.start_date.date())

        # Remove the previous days temp files.
        self.tmp_files.remove()

        check_ids = self.get_multi_url_check_ids()

        if len(check_ids) == 0:
            self.inform_luigi_processing_completed()
            return

        for check_id in check_ids:
            url = self.get_url(processing_time_span, check_id[0])
            json_dict_summary_result = self.get_data(url, check_id[0])

            if json_dict_summary_result is not None and len(json_dict_summary_result) > 0:
                self.extract_performance.run(json_dict_summary_result, self.start_date.date())
            else:
                # Write empty performance data since there is no header info for the check id.
                empty_result = []
                fact_path = self.extract_performance.get_file_path(self.start_date.date(), check_id[0])
                self.save_data(fact_path, empty_result)

            # Save the header results.
            full_path = self.get_file_path(check_id[0])
            self.save_data(full_path, json_dict_summary_result)

        self.pl.tasks_insert(self.load_date, self.start_date)

    def get_processing_timespan(self, start_date=''):

        if start_date == '':
            start_date = (dt.datetime.today() - dt.timedelta(days=1)).date()

        date_time_span = dict()
        date_time_span['start_day'] = start_date
        date_time_span['end_day'] = start_date
        date_time_span['to_hour'] = 'T23:59:59'
        date_time_span['from_hour'] = 'T00:00:00'

        return date_time_span

    def get_multi_url_check_ids(self):
        """
        Obtain the Check IDs that have multiple URLs associated with them.
        :return: List of check IDs
        """

        try:
            check_id_list = self.pw_src_cur.execute(var.GET_MULTI_URL_CHECKS).fetchall()

        except Exception as e:
            error_msg = var.GET_MULTI_URL_CHECKS_ERROR_MSG.format(e)
            self.pl.logger.error(error_msg)
            sys.exit(error_msg)

        return check_id_list

    def get_url(self, processing_time_span, check_id):
        """
        Builds the URL.
        :param processing_time_span: Time block for desired data.
        :param check_id: Check Id for the particular client.
        :return: URL that constitutes the Get API call.
        """
        ts_for_api = var.UTC_TIMESPAN.format(processing_time_span['start_day'],
                                             processing_time_span['from_hour'],
                                             processing_time_span['end_day'],
                                             processing_time_span['to_hour'])

        url = var.URL_QUERY_INFO.format(var.BASE_URL,
                                        str(check_id),
                                        ts_for_api,
                                        var.AUTH_TICKET)
        return url

    def get_data(self, url, check_id):
        """
        Executes the Get API call.
        :param url: URL that constitutes the Get API call.
        :param check_id: Check Id for the particular client.
        :return: Site performance data for the particular client.
        """
        try:
            response = urllib.request.urlopen(url)
        except Exception as e:
            error_msg = "FAILED to obtain data from URL. ERROR MESSAGE: {} --URL: {} ".format(e, url)
            self.pl.logger.exception(error_msg)
            self.apica_check_status.determine_check_id_status(check_id)
            return None

        # The HTTP call returns a JSON array of monitors.
        json_list_result = response.read()
        json_dict_result = json.loads(json_list_result.decode('utf-8'))

        if not json_dict_result:
            self.pl.logger.info("No data to process.")
            return None

        return json_dict_result

    def get_file_path(self, check_id):
        prefix = var.SUMMARIES_HEADERS.format(check_id)
        filename = get_filename(self.start_date.date(), prefix, 'json')
        return os.path.join(var.DATA_SITES_PTH, filename)

    def save_data(self, full_path, json_dict_result):
        with open(full_path, 'w') as outfile:
            json.dump(json_dict_result, outfile)


class TransformLoadSites(luigi.Task):
    start_date = luigi.DateSecondParameter(default=(dt.datetime.today() - dt.timedelta(days=1)))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("Load2Sites", "TransformSitesSummary")

    site_manager = SiteManager()
    conn = Connection()
    pw_src_cur = conn.pw_src.return_cursor()
    plat_cur = conn.plat_src.return_cursor()
    pw_tar_cur = conn.pw_tar.return_cursor()
    pw_tar_sa = conn.sa_create_engine(conn.pw_tar_cstr)
    pw_src_conn = conn.pw_src.return_conn()

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.start_date.date())

    def requires(self):
        return ExtractSitesSummary(start_date=self.start_date, load_date=self.load_date)

    def run(self):
        check_ids = self.get_multi_url_check_ids()

        url_org_id_mappings_df = self.get_url_org_id_mappings()

        for check_id in check_ids:
            # Process Dim information for check id.
            header_data = self.get_data(check_id[0], 'headers')
            if header_data is not None:
                headers = []
                for dict_data in header_data:
                    if self.is_valid_header(dict_data):
                        headers.append(self.transform_header_data(dict_data))
                transformed_headers = pd.DataFrame(headers, columns=var.HEADER_COLUMNS)
                is_deleted = self.delete_dim_records(transformed_headers['ResultID'])
                if is_deleted:
                    self.load_data(transformed_headers, var.DIM_SITES_TABLE)

                # Process Fact information for check id.

                performance_data = self.get_data(check_id[0], 'performance')
                performance_results = self.transform_performance_data(performance_data, url_org_id_mappings_df)
                transformed_results = pd.DataFrame(performance_results, columns=var.SITES_COLUMNS)
                is_facts_deleted = self.delete_fact_records(transformed_results['ResultID'].iloc[0])
                if is_facts_deleted:
                    self.load_data(transformed_results, var.FACT_SITES_TABLE)

        self.pl.tasks_insert(self.load_date, self.start_date)

    def get_data(self, check_id, file_type):
        """
        Get the data from previously saved json files.
        :param check_id:
        :param file_type: header file or performance file (details)
        :return: Json object.
        """
        prefix = ''
        if file_type == 'headers':
            prefix = 'Summaries-Headers-{}'.format(check_id)
        else:
            prefix = 'SitesPerformance-{}'.format(check_id)

        filename = get_filename(self.start_date.date(), prefix, 'json')
        full_path = os.path.join(var.DATA_SITES_PTH, filename)

        data = []
        with open(full_path) as data_file:
            data = json.load(data_file)

        return data

    def delete_dim_records(self, result_ids):
        """
        If this date is being re-run the existing Dim records must deleted.
        :param result_ids: List of Result IDs
        :return: True is deletion was successful, otherwise False. This is to ensure that the insertion is not
        executed if an error occurs.
        """
        if len(result_ids) == 0:
            return True

        # Build the list for the IN clause.
        in_values = ''
        for rid in result_ids:
            in_values = in_values + "'{}',".format(rid)

        # Drop the last ',' from the string.
        in_values = in_values[0:len(in_values)-1]

        try:
            self.pw_tar_cur.execute(var.DELETE_DIM_SITES.format(in_values)).commit()
        except Exception as e:
            error_msg = "Unable to delete from DimSites table. ResultIDs: {}. ERROR Message: {}"\
                .format(in_values, e)
            self.pl.logger.exception(error_msg)
            return False

        return True

    def delete_fact_records(self, result_id):
        """
        If this date is being re-run the existing fact records must deleted.
        :param result_id: Result ID being processed.
        :return: True is deletion was successful, otherwise False. This is to ensure that the insertion is not
        executed if an error occurs.
        """
        try:
            self.pw_tar_cur.execute(var.DELETE_SITES.format(result_id)).commit()
        except Exception as e:
            error_msg = "Unable to delete from FactSites table. ResultID: {}. ERROR Message: {}"\
                .format(result_id, e)
            self.pl.logger.exception(error_msg)
            return False

        return True

    def load_data(self, df, table_name):
        """
        Saves records to the database.
        :param df: The pandas dataframe that is to be saved.
        :param table_name: The table the data is to be saved to.
        :return: True if the records were successfully saved.
        """
        try:
            df.to_sql(table_name,
                      schema='dw',
                      if_exists='append',
                      con=self.pw_tar_sa,
                      index=None)
        except Exception as e:
            err_msg = "ERROR occurred while inserting new checks to StgChecks table. Actual message: {}".format(e)
            self.pl.logger.error(err_msg)
            sys.exit(err_msg)

        return True

    def get_multi_url_check_ids(self):
        """
        Obtain the Check IDs that have multiple URLs associated with them.
        :return: List of check IDs
        """

        try:
            check_id_list = self.pw_src_cur.execute(var.GET_MULTI_URL_CHECKS).fetchall()

        except Exception as e:
            error_msg = var.GET_MULTI_URL_CHECKS_ERROR_MSG.format(e)
            self.pl.logger.error(error_msg)
            sys.exit(error_msg)

        return check_id_list

    def is_valid_header(self, dict_data):
        """
        Validates the numeric Dim data returned.
        :param dict_data: A row of Dim data.
        :return: True if all fields contain digits.otherwise, False.
        """
        is_valid = True

        if 'check_id' in dict_data and not type(dict_data['check_id']) is int:
            is_valid = False
        if 'value' in dict_data and not type(dict_data['value']) is int:
            is_valid = False
        if 'result_code' in dict_data and not type(dict_data['result_code']) is int:
            is_valid = False
        if 'attempts' in dict_data and not type(dict_data['attempts']) is int:
            is_valid = False

        return is_valid

    def transform_performance_data(self, performance_data, url_org_id_mappings_df):
        """
        Validates and transforms the Performance data so that it can be saved to the database.
        If Check_ID, Result Id, TimestampUTC contains invalid data the entire set is rejected. If the url
        is not provided the performance result for that record is not saved.
        :param performance_data: Performance data for a specified Result ID
        :param url_org_id_mappings_df: url to organization mapping.
        :return: Transformed data.
        """

        processed_check_results = []

        data = performance_data[0]
        check_results = data['check_results']

        for check_result in check_results:
            is_valid_check_result = True

            check_id = 0
            if 'check_id' in check_result and type(check_result['check_id']) is int:
                check_id = check_result['check_id']
            else:
                is_valid_check_result = False

            result_id = ''
            if 'result_id' in check_result and check_result['result_id'] is not None:
                result_id = check_result['result_id'][0:60]
            else:
                is_valid_check_result = False

            time_stamp_utc = ''
            if 'time_stamp_utc' in check_result and check_result['time_stamp_utc'] is not None:
                time_stamp_utc = check_result['time_stamp_utc']
            else:
                is_valid_check_result = False

            url_results = check_result['url_results']

            if not is_valid_check_result or len(url_results) == 0:
                return processed_check_results

            for url_result in url_results:

                is_valid_record = True
                url_number = -1
                if 'url_number' in url_result and type(url_result['url_number']) is int:
                    url_number = url_result['url_number']

                url_domain_name = ''
                url_hash = 0x00
                if 'url' in url_result and url_result['url'] is not None:
                    url = url_result['url'][0:2083]
                    url_domain_name = self.get_url_name(url)
                    url_domain_name = url_domain_name.lower().strip()
                    url_hash = self.site_manager.get_hash(url_domain_name)
                else:
                    is_valid_record = False

                organization_id = 0
                tenant_id = 0

                if len(url_domain_name) > 0:
                    result = url_org_id_mappings_df[url_org_id_mappings_df['URL'] == url_domain_name]
                    if len(result) > 0:
                        organization_id = result['OrganizationID']
                        tenant_id = result['ParentID']

                elapsed_ms = -1
                if 'elapsed_ms' in url_result and type(url_result['elapsed_ms']) is int:
                    elapsed_ms = url_result['elapsed_ms']

                received_bytes = -1
                if 'received_bytes' in url_result and type(url_result['received_bytes']) is int:
                    received_bytes = url_result['received_bytes']

                http_method = 'Not Provided'
                if 'http_method' in url_result and url_result['http_method'] is not None:
                    http_method = url_result['http_method']

                http_status_code = -1
                if 'http_status_code' in url_result and type(url_result['http_status_code']) is int:
                    http_status_code = url_result['http_status_code']

                dns_lookup_duration_ms = -1
                if 'dns_lookup_duration_ms' in url_result and type(url_result['dns_lookup_duration_ms']) is int:
                    dns_lookup_duration_ms = url_result['dns_lookup_duration_ms']

                connect_duration_ms = -1
                if 'connect_duration_ms' in url_result and type(url_result['connect_duration_ms']) is int:
                    connect_duration_ms = url_result['connect_duration_ms']

                send_duration_ms = -1
                if 'send_duration_ms' in url_result and type(url_result['send_duration_ms']) is int:
                    send_duration_ms = url_result['send_duration_ms']

                wait_duration_ms = -1
                if 'wait_duration_ms' in url_result and type(url_result['wait_duration_ms']) is int:
                    wait_duration_ms = url_result['wait_duration_ms']

                receive_duration_ms = -1
                if 'receive_duration_ms' in url_result and type(url_result['receive_duration_ms']) is int:
                    receive_duration_ms = url_result['receive_duration_ms']

                headers = 'Not Provided'
                if 'headers' in url_result and url_result['headers'] is not None:
                    tmp = str(url_result['headers'])
                    headers = tmp[0:2083]

                multiple_timings = 'Not Provided'
                if 'multiple_timings' in url_result and url_result['multiple_timings'] is not None:
                    multiple_timings = str(url_result['multiple_timings'])

                if is_valid_record:
                    result = {
                        "CheckID": check_id,
                        "ResultID": result_id,
                        "URLNumber": url_number,
                        "URL": url_domain_name,
                        "URLHash": url_hash,
                        "TenantID": int(tenant_id),
                        "OrganizationID": int(organization_id),
                        "TimestampUTC": time_stamp_utc,
                        "ElapsedMS": elapsed_ms,
                        "ReceivedBytes": received_bytes,
                        "HTTPMethod": http_method,
                        "HTTPStatusCode": http_status_code,
                        "DNSLookupDurationMS": dns_lookup_duration_ms,
                        "ConnectDurationMS": connect_duration_ms,
                        "SendDurationMS": send_duration_ms,
                        "WaitDurationMS": wait_duration_ms,
                        "ReceiveDurationMS": receive_duration_ms,
                        "Headers": headers,
                        "MultipleTimings": multiple_timings
                    }
                    processed_check_results.append(result)

        return processed_check_results

    def transform_header_data(self, dict_data):

        check_id = 0
        if 'check_id' in dict_data:
            check_id = int(dict_data['check_id'])

        result_id = ''
        if 'identifier' in dict_data:
            result_id = dict_data['identifier'][0:60]

        message = ''
        if 'message' in dict_data:
            message = dict_data['message']

        attempts = 0
        if 'attempts' in dict_data:
            attempts = int(dict_data['attempts'])

        result_code = 0
        if 'result_code' in dict_data:
            result_code = int(dict_data['result_code'])

        timestamp_utc = ''
        if 'timestamp_utc' in dict_data:
            timestamp_utc = dict_data['timestamp_utc']

        severity = ''
        if 'severity' in dict_data:
            severity = dict_data['severity']

        value = 0
        if 'value' in dict_data:
            value = dict_data['value']

        unit = ''
        if 'unit' in dict_data:
            unit = dict_data['unit']

        header = {
            "ResultID": result_id,
            "CheckID": check_id,
            "TimeStampUTC": timestamp_utc,
            "Message": message,
            "Attempts": attempts,
            "ResultCode": result_code,
            "Severity": severity,
            "Value": value,
            "Unit": unit
        }

        return header

    def get_url_org_id_mappings(self):
        try:
            url_orgid_mappings_tuples = self.plat_cur.execute(var.GET_URL_ORGID_MAPPINGS_WITH_HASH).fetchall()

        except Exception as e:
            error_msg = var.URL_ORGID_MAPPINGS_ERROR_MSG.format(e)
            self.pl.logger.error(error_msg)
            sys.exit(error_msg)

        # Convert to lists.
        mapping_list = []
        for row in url_orgid_mappings_tuples:
            a_row = list()
            a_row.append(row[0])
            a_row.append(row[1])
            a_row.append(row[2])
            mapping_list.append(a_row)

        mappings = pd.DataFrame(mapping_list, columns=var.URL_ORGID_MAPPINGS_COLUMNS)
        return mappings

    def get_url_name(self, url):
        """
        Remove the http and other stuff from the url.
        """
        if len(url) == 0:
            return ''

        url_info = urllib.parse.urlsplit(url)

        return url_info.hostname.rstrip()


class RunTasks(luigi.Task):
    """
    Main entrypoint for this module.
    """
    start_date = luigi.DateSecondParameter(default=(dt.datetime.today() - dt.timedelta(days=1)))
    load_date = luigi.DateSecondParameter(default=dt.datetime.today())

    pl = Pipeline("Load2Sites", "RunTasks")

    def output(self):
        return PipelineTasks(self.pl.etl_name, self.pl.task_name, self.load_date.date(), self.start_date.date())

    def run(self):
        try:
            yield [TransformLoadSites(start_date=self.start_date, load_date=self.load_date)]

        except Exception as e:
            sys.exit(e)

        self.pl.log_insert(self.load_date, self.start_date)


def get_filename(start_date, filename, extension):

    if type(start_date is not str):
        start_date = str(start_date)

    start_date = start_date.replace('-', '')
    if len(start_date) > 0:
        return '{}-{}.{}'.format(filename, start_date, extension)
    else:
        return '{}.{}'.format(filename, extension)


if __name__ == '__main__':
    lu = luigi.run()

    if lu is not True:
        slack_msg("LoadSites", "LoadSites ETL has failed.")

