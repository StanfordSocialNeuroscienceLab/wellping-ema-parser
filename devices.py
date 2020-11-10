#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

"""
Hacked together by I. Richard Ferguson
11.09.20 | San Francisco, CA

Helper functions for ripper.py EMA parser
Produces CSV output of user device / software information
"""

import pandas as pd
import os


def parseDevices(inData, user):
    """"
    Loops through participant response dictionaries
    Isolates user device data (e.g., Apple iPhone XR, iOS version xx.xx)
    Concatenates values to master device output CSV
    """

    temp = pd.DataFrame()                                                                   # Empty DF to append into

    for id in list(inData.keys()):
        sparse = inData[id]["user"]                                                         # Isolate device + app data
        device = pd.DataFrame(sparse['installation']).loc[:, "device"].reset_index()
        app = pd.DataFrame(sparse['installation']).loc[:, 'app'].reset_index()
        app.columns = ['index', 'device']                                                   # Rename columns to match
        all_data = pd.concat([device, app]).dropna().reset_index(drop=True)                 # Combine above DFs
        all_data.columns = ['var', 'device']                                                # Rename output columns
        all_data['user'] = user                                                             # Impute username into DF
        all_data_long = all_data.pivot(columns="var", values="device", index="user")        # Long to wide pivot
        all_data_long['user'] = user
        temp = temp.append(all_data_long, ignore_index=True, sort=False)                    # Append to output DF

    return temp


def deviceCleanup(DF):
    """
    Fills null values and reorganizes device DF
    """

    DF.fillna("NA", inplace=True)                                                           # Fill NA values
    columns = ["user", "brand", "osName", "osVersion"]                                      # Left-most columns
    extra_vars = []

    for var in DF.columns:
        if var not in columns:
            extra_vars.append(var)
        else:
            continue

    extra_vars.sort()
    columns.extend(extra_vars)                                                              # List of all vars
    DF = DF[columns]                                                                        # Reorder DF columns
    return DF


def pushDevices(DF, output_directory, output_name):
    """
    Pushes cleaned device DF to output directory
    """

    clean = deviceCleanup(DF)                                                               # Clean device DF w/ helper
    os.chdir(output_directory)                                                              # Direct DF to output dir
    output_name = "{}_deviceInfo.csv".format(output_name)                                   # Format file name
    clean.to_csv(output_name, index=False)                                                  # Push CSV to directory
