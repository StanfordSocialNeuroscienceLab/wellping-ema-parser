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
    Fill
    """

    temp = pd.DataFrame()

    for id in list(inData.keys()):
        sparse = inData[id]["user"]

        device = pd.DataFrame(sparse['installation']).loc[:, "device"].reset_index()
        app = pd.DataFrame(sparse['installation']).loc[:, 'app'].reset_index()
        app.columns = ['index', 'device']
        all_data = pd.concat([device, app]).dropna().reset_index(drop=True)
        all_data.columns = ['var', 'device']
        all_data['user'] = user

        all_data_long = all_data.pivot(columns="var", values="device", index="user")
        all_data_long['user'] = user

        temp = temp.append(all_data_long, ignore_index=True, sort=False)

    return temp


def deviceCleanup(DF):
    """
    Fill
    """

    DF.fillna("NA", inplace=True)

    columns = ["user", "brand", "osName", "osVersion"]
    extra_vars = []

    for var in DF.columns:
        if var not in columns:
            extra_vars.append(var)
        else:
            continue

    extra_vars.sort()
    columns.extend(extra_vars)

    DF = DF[columns]

    return DF


def pushDevices(DF, output_directory, output_name):
    """
    Fill
    """

    clean = deviceCleanup(DF)
    os.chdir(output_directory)
    output_name = "{}_deviceInfo.csv".format(output_name)
    clean.to_csv(output_name, index=False)
