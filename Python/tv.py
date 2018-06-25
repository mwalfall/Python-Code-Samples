import datetime

# Simulates streaming SetBox data. Objective to determine how long someone is watching NBC.

# ASSUMPTIONS:
# The tuples are sorted in Id, datetime order.

# Test conditions:
# 45633: Two NBC events occur. One before the time period and ends before time period. Not valid
#        The second event occurs prior to the time period but ends during time period. Valid
events = [(45633, 'FOX', datetime.datetime(2018, 01, 22, 03, 44)),
          (45633, 'NBC', datetime.datetime(2018, 01, 22, 05, 22)),  # starts at 05:22
          (45633, 'A&E', datetime.datetime(2018, 01, 22, 05, 44)),  # ends at 05:44  -- Not valid
          (45633, 'NBC', datetime.datetime(2018, 01, 22, 14, 00)),  # starts at 14:00
          (45633, 'ABC', datetime.datetime(2018, 01, 22, 15, 30)),  # ends at 15:30  -- valid
          (45633, 'CBS', datetime.datetime(2018, 01, 22, 17, 1)),
          # 60000: Starts early and ends before the time period. Not valid.
          (60000, 'A&E', datetime.datetime(2018, 01, 22, 05, 44)),
          (60000, 'NBC', datetime.datetime(2018, 01, 22, 6, 00)),  # starts at 06:00
          (60000, 'ABC', datetime.datetime(2018, 01, 22, 8, 30)),  # ends at 08:30  -- not valid
          (60000, 'CBS', datetime.datetime(2018, 01, 22, 17, 1)),
          # 70000: Two valid conditions within the time period. Only one should be recorded.
          (70000, 'FOX', datetime.datetime(2018, 01, 22, 03, 44)),
          (70000, 'NBC', datetime.datetime(2018, 01, 22, 14, 22)),  # starts at 14:22
          (70000, 'A&E', datetime.datetime(2018, 01, 22, 15, 44)),  # ends at 15:44  -- Not valid
          (70000, 'NBC', datetime.datetime(2018, 01, 22, 16, 00)),  # starts at 16:00
          (70000, 'ABC', datetime.datetime(2018, 01, 22, 16, 30)),  # ends at 13:30  -- valid
          (70000, 'CBS', datetime.datetime(2018, 01, 22, 17, 1)),
          # 77000: Two events within the time period but they are less than 5 minutes in length.
          (77000, 'FOX', datetime.datetime(2018, 01, 22, 03, 44)),
          (77000, 'NBC', datetime.datetime(2018, 01, 22, 15, 22)),  # starts at 15:22
          (77000, 'A&E', datetime.datetime(2018, 01, 22, 15, 23)),  # ends at 15:23  -- Not valid
          (77000, 'NBC', datetime.datetime(2018, 01, 22, 16, 00)),  # starts at 16:00
          (77000, 'ABC', datetime.datetime(2018, 01, 22, 16, 04)),  # ends at 16:04  -- Not valid
          (77000, 'CBS', datetime.datetime(2018, 01, 22, 17, 1)),
          # 78000:  Two events with one starting within the time period ending outside the time period.
          (78000, 'FOX', datetime.datetime(2018, 01, 22, 03, 44)),
          (78000, 'NBC', datetime.datetime(2018, 01, 22, 15, 22)),  # starts at 15:22
          (78000, 'A&E', datetime.datetime(2018, 01, 22, 15, 23)),  # ends at 15:23  -- Not valid
          (78000, 'NBC', datetime.datetime(2018, 01, 22, 16, 54)),  # starts at 16:54
          (78000, 'ABC', datetime.datetime(2018, 01, 22, 17, 04)),  # ends at 17:04  -- valid
          (78000, 'CBS', datetime.datetime(2018, 01, 22, 17, 1)),
          # 79000: valid event that starts before the time span and ends after the time span.
          (79000, 'FOX', datetime.datetime(2018, 01, 22, 03, 44)),
          (79000, 'NBC', datetime.datetime(2018, 01, 22, 15, 22)),  # starts at 15:22
          (79000, 'A&E', datetime.datetime(2018, 01, 22, 15, 23)),  # ends at 15:23  -- Not valid
          (79000, 'NBC', datetime.datetime(2018, 01, 22, 15, 40)),  # starts at 15:40
          (79000, 'ABC', datetime.datetime(2018, 01, 22, 17, 17)),  # ends at 17:17  -- valid
          (79000, 'CBS', datetime.datetime(2018, 01, 22, 17, 1)),
          # 80000: the last tuple is a NBC event. However, there isn't another event following it.
          (80000, 'FOX', datetime.datetime(2018, 01, 22, 03, 44)),
          (80000, 'NBC', datetime.datetime(2018, 01, 22, 15, 22)),  # starts at 15:22
          (80000, 'A&E', datetime.datetime(2018, 01, 22, 15, 23)),  # ends at 15:23  -- Not valid
          (80000, 'NBC', datetime.datetime(2018, 01, 22, 15, 40)),  # User started viewing within the time block
          (81000, 'FOX', datetime.datetime(2018, 01, 22, 03, 40)),  # An ending record is not present.
          (81000, 'NBC', datetime.datetime(2018, 01, 22, 16, 58)),  # Last record is NBC but 2 minutes. So Invalid.
          # End of event id test.
          # 83000: event starts within the time span and there is no ending record.
          (83000, 'FOX', datetime.datetime(2018, 01, 22, 03, 44)),
          (83000, 'NBC', datetime.datetime(2018, 01, 22, 15, 22)),  # starts at 15:22
          (83000, 'A&E', datetime.datetime(2018, 01, 22, 15, 23)),  # ends at 15:23  -- Not valid
          (83000, 'NBC', datetime.datetime(2018, 01, 22, 16, 44)),  # starts at 15:40 -- Valid record.
          ]

# Dictionary of tuples that keeps track of last processed event for each id.
# KEY: id
# CONTENT: tuple: (tv station, starting time of the event)
unique_event_ids = dict()

# Dictionary of tuples that records the events that meet the viewing condition.
# KEY: id
# CONTENT: tuple: (tv station, starting time of the event)
unique_nbc_events = dict()

# Time block
start_time = datetime.datetime(2018, 01, 22, 15, 00)  # 15:00
end_time = datetime.datetime(2018, 01, 22, 17, 00)    # 17:00


def process_nbc_event(event_id, present_event_time, previous_recorded_event):
    '''
    This function determines if an NBC event meets the following conditions:
    * The station was viewed for at least five minutes.
    * The station was view within the specified start and end time block.
    * If this conditions are met, the last event to meet this condition is recorded in
      the dictionary unique_nbc_events.
    :param event_id: id of the present event being processed.
    :param present_event_time: the time of th present event that is being processed.
    :param previous_recorded_event: the previously recorded event.
    '''

    # If NBC was viewed for more than 5 minutes check to see if the viewing
    # time fell within the specified time slot.
    elapsed_time = present_event_time - previous_recorded_event[1]
    if elapsed_time >= datetime.timedelta(minutes=5):
        if start_time <= previous_recorded_event[1] <= end_time:
            unique_nbc_events[event_id] = (previous_recorded_event[0], previous_recorded_event[1])

        if previous_recorded_event[1] < start_time < previous_recorded_event[1] + elapsed_time:
            unique_nbc_events[event_id] = (previous_recorded_event[0], previous_recorded_event[1])


def process_events():
    '''
    Main processing loop. Determines if there is an NBC event that should be processed.
    Returns: last_event_id which contains the id of the last event processed.
    '''
    last_event_id = 0

    for event in events:
        # If there is a change in the id check to see if the last record was a NBC event.
        if last_event_id > 0 and event[0] != last_event_id:
            process_last_event(last_event_id)

        if event[0] in unique_event_ids:
            previously_recorded_event = unique_event_ids[event[0]]
            if previously_recorded_event[0].lower() == 'nbc' and event[1].lower() != 'nbc':
                process_nbc_event(event[0], event[2], previously_recorded_event)

        unique_event_ids[event[0]] = (event[1], event[2])
        last_event_id = event[0]

    return last_event_id


def process_last_event(event_id):
    '''
    If the last event in the events list was a NBC event we have to check if it should be recorded.
    This covers the situation where the station was viewed with the timespan but an ending event
    is not present. For the event to be recorded it must start at least 5 minutes before the
    end of the time block. In this case, 17:00.
    :param event_id: Id of the last event processed.
    '''

    last_event = unique_event_ids[event_id]
    if last_event[0].lower() == 'nbc':
        process_nbc_event(event_id, end_time, last_event)


def print_results():
    print('Number of unique events within time period: {}'.format(len(unique_nbc_events)))
    print('Details: {}'.format(unique_nbc_events))


id_of_last_event = process_events()
process_last_event(id_of_last_event)
print_results()

