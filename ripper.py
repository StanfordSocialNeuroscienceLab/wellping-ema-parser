#! /usr/bin/env python3.6
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
from parser import *                                                    # Cowboy Emoji

# --------- FILE SYSTEM HIERARCHY
here = os.getcwd()                                                      # Define local file path
there = here + "/" + sys.argv[1] + "/"
os.chdir(there)
setup(there)                                                            # Build these directories if they don't exist
participantDirectory = here + "/89_Participant-Files/"                  # Define landing directory for individual CSVs
compositeDirectory = here + "/99_Composite-CSV/"                        # Define landing directory for final output


# --------- READ IN JSON
temp_name=grabJSON(there)

with open(temp_name, "r") as incoming:
    data=json.load(incoming)
    data=data.copy()


# -------- LOOP THROUGH PARSER FUNCTION
output_name=temp_name[:-5]
log = open("{}.txt".format(output_name), "w")
keys_outer = list(data.keys())                                          # Isolate participant user names

for ix in tqdm(range(len(keys_outer))):                                 # Loop through user names and parse data
    temp = data[keys_outer[ix]]                                         # See wrapper for helper function implementation
    username = keys_outer[ix]
    parseReponses(temp, username, log)

print("All participants' data parsed...")
log.close()
sleep(1)
print("\nCombining all files...")
sleep(1)

output(output_name)

print("\nAll files combined!")
