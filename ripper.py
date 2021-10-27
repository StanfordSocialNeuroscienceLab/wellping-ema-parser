#!/bin/python3

"""
WellPing EMA Parser - Wrapper Script

About this script
    * Run `python3 ripper.py { target_directory }`
    * This script will create individual CSVs for each subject in the 89 directory
    * An aggregated CSV is saved in the 99 directory

Ian Richard Ferguson | Stanford University
"""

# ---------- IMPORTS
import json, sys, os
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
compositeDirectory = os.path.join(there, "99_Composite-CSV")            # Define landing directory for final output


# --------- READ IN JSON
os.chdir(there)
temp_name=iso_JSON(there)

with open(temp_name, "r") as incoming:
    data=json.load(incoming)
    data=data.copy()


# -------- LOOP THROUGH PARSER FUNCTION
output_name=temp_name[:-5]

with open(f"{output_name}.txt", "w") as log:
    keys_outer = list(data.keys())                                      # Isolate participant user names
    parent_errors = []

    print("\nParsing EMA responses....")
    for ix in tqdm(range(len(keys_outer))):                             # Loop through user names and parse data
        temp = data[keys_outer[ix]]                                     # See wrapper for helper function implementation
        username = keys_outer[ix]

        try:
            parse_responses(temp, username, log)
        except Exception as e:
            print(e)
            # parent_errors.append(username)
            continue

    print("\nAll participants' data parsed...")

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
    devices = devices.append(parse_devices(temp, username),              # Parse participant device info + append to DF
                             ignore_index=True, sort=False)

push_devices(devices,                                                    # Push devices CSV to output directory
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
