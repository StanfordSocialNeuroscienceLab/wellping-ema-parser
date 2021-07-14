#!/bin/python3
# -*- coding: utf-8 -*-

"""
Hacked together by I. Richard Ferguson
10.02.20 | San Francisco, CA

User Notes: At the command line run "python3.6 ripper.py {TARGET DIRECTORY}"
This program will do the rest!
"""

# ---------- IMPORTS
import json
import sys
import os
import pandas as pd
from parser import *                                                    # Cowboy Emoji
from devices import *

# --------- FILE SYSTEM HIERARCHY
there = os.path.join("{}/{}/".format(os.getcwd(), sys.argv[1]))         # Output directory

if not os.path.isdir(there):
    print("\n{} is invalid path...".format(there))                      # Confirm path is correct
    sys.exit(1)

setup(there)                                                            # Build these directories if they don't exist
participantDirectory = os.path.join(there, "89_Participant-Files")      # Define landing directory for individual CSVs
compositeDirectory = os.path.join(there, "/99_Composite-CSV/")          # Define landing directory for final output


# --------- READ IN JSON
os.chdir(there)
temp_name=grabJSON(there)

with open(temp_name, "r") as incoming:
    data=json.load(incoming)
    data=data.copy()


# -------- LOOP THROUGH PARSER FUNCTION
output_name=temp_name[:-5]
log = open("{}.txt".format(output_name), "w")
keys_outer = list(data.keys())                                          # Isolate participant user names
parent_errors = []

print("\nParsing EMA responses....")
for ix in tqdm(range(len(keys_outer))):                                 # Loop through user names and parse data
    temp = data[keys_outer[ix]]                                         # See wrapper for helper function implementation
    username = keys_outer[ix]

    try:
        parseReponses(temp, username, log)
    except Exception as e:
        parent_errors.append(username)
        continue

print("\nAll participants' data parsed...")
log.close()
sleep(1)
print("\nCombining all files...")
sleep(1)

output(output_name)                                                     # Push clean CSV to output directory

# -------- LOOP THROUGH DEVICE ID FUNCTION
devices = pd.DataFrame()                                                # Empty devices DF to append into
print('\nScraping device information...')

for ix in tqdm(range(len(keys_outer))):
    temp = data[keys_outer[ix]]
    username = keys_outer[ix]
    devices = devices.append(parseDevices(temp, username),              # Parse participant device info + append to DF
                             ignore_index=True, sort=False)

pushDevices(devices,                                                    # Push devices CSV to output directory
            output_directory=(there + "/99_Composite-CSV/"),
            output_name=output_name)

try:
    with open("parent_errors.json", "w") as outgoing:
        print("\nSaving JSON file containing existential errors...")
        temp = {sub: data[sub] for sub in parent_errors}
        json.dump(temp, outgoing, indent=4)
except:
    print("You're here:\t\t{}".format(os.getcwd()))

print("\nAll files combined!")
