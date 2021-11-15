#!/bin/python3

"""
About this Script

"""


# ----- Imports
import pandas as pd
import os


# ----- Functions
def parse_device_info(SUBSET):
    """
    
    """

    devices = SUBSET['user']                                            #
    username = devices['username']                                      #

    master = pd.DataFrame({'username':username}, index=[0])             #

    for key in ['device', 'app']:
        temp = devices['installation'][key]                             #
        temp_frame = pd.DataFrame(temp, index=[0])                      #
        temp_frame['username'] = username                               #
        
        master = master.merge(temp_frame, on='username')                #

    return master