#!/bin/python3

"""
About this Script

This helper function flattens participant subject data into a one-row
DataFrame. This info is parsed for each subject and saved locally as a CSV

Ian Ferguson | Stanford University
"""


# ----- Imports
import pandas as pd


# ----- Functions
def parse_device_info(SUBSET, KEY):
    """
    SUBSET => Particpant's reduced JSON file (as Python dictionary)
    KEY => Key from the JSON data dictionary

    This function flattens user device info into a single-row
    DataFrame object. This is returned and stacked with others
    in the main function

    Returns DataFrame object
    """

    devices = SUBSET['user']                                            # Isolate device information from JSON
    username = devices['username']                                      # Pull in username from data dictionary
    login_time = KEY.split('-')[-1]                                     # Isolate subject login time from data key


    master = pd.DataFrame({'username':username}, index=[0])             # Parent DF to merge into

    for key in ['device', 'app']:
        temp = devices['installation'][key]                             # Isloate sub-keys from dictionary
        temp_frame = pd.DataFrame(temp, index=[0])                      # Flatten wide to long
        temp_frame['username'] = username                               # Isolate username
        temp_frame['login_time'] = login_time                           # JavaScript derived login time
        
        master = master.merge(temp_frame, on='username')                # Merge with parent DF on username

    return master