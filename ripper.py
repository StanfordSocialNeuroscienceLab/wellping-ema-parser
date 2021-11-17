#!/bin/python3

"""
About this Script

This script wraps helper functions defined in `parser.py` and
is fully-executable from the command line

NOTE: run the following at the command line `python3 ripper.py { target_directory }

Ian Ferguson | Stanford University
"""

"""
Running To-Do

* Duplication issue (same as over the summer ... see how we fixed that on safe branch)
"""

# ----- Imports
import os, sys, json
from time import sleep
from tqdm import tqdm
import pandas as pd
from parser import agg_drop_duplicates, setup, isolate_json_file, parse_responses
from devices import parse_device_info


# ----- Run Script
def main():
      target_path = sys.argv[1]                                               # Isolate relative path to data
      setup(target_path)                                                      # Create output directories
      sub_data, output_filename = isolate_json_file(target_path)              # Isolate JSON file

      # These output directories will hold parsed data
      subject_output_directory = os.path.join(".", target_path, "00-Subjects")
      aggregate_output_directory = os.path.join(".", target_path, "01-Aggregate")

      # I/O JSON file
      with open(sub_data) as incoming:

            # I/O new text file for exception logging
            with open(f"./{target_path}/{output_filename}.txt", "w") as log:
                  
                  data = json.load(incoming)                                  # Read JSON as Python dictionary

                  keepers = []                                                # Empty list to append subject data into
                  parent_errors = {}                                          # Empty dictionary to append sparse data into

                  print("\nParsing participant data...\n")
                  sleep(1)

                  # Key == Subject and login ID (we'll separate these later)
                  for key in tqdm(list(data.keys())):

                        subset = data[key]                                    # Reduced data for one participant

                        # If participant completed no pings, push them to parent dict
                        if len(subset['answers']) == 0:
                              parent_errors[key] = subset
                              continue
                        
                        try:

                              # Run parse_responses function to isolate participant data
                              parsed_data = parse_responses(key, subset, log, 
                                                            subject_output_directory, True)

                        except Exception as e:

                              # Catch exceptions as they occur
                              log.write(f"\nCaught @ {key.split('-')[0]}: {e}\n\n")
                              continue

                        keepers.append(parsed_data)                           # Add participant DF to keepers list

                  sleep(1)
                  print("\nAggregating participant data...\n")

                  try:

                        # Stack all DFs into one
                        aggregate = pd.concat(keepers)

                        aggregate = agg_drop_duplicates(aggregate)

                        # Push to local CSV
                        aggregate.to_csv(f'./{target_path}/01-Aggregate/pings_{output_filename}.csv',
                                          index=False, encoding="utf-8-sig")

                  except Exception as e:

                        # Something has gone wrong here and you have no participant data ... check the log
                        print(f"{e}")
                        print("\nNo objects to concatenate...\n")
                        sys.exit(1)

                  print("\nSaving parent errors...\n")

                  # Push parent errors (no pings) to local JSON
                  with open(f'./{target_path}/01-Aggregate/parent-errors.json', 'w') as outgoing:
                        json.dump(parent_errors, outgoing, indent=4)

                  print("\nParsing device information...\n")

                  # I/O new text file for device parsing errors
                  with open(f"./{target_path}/device-error-log.txt", 'w') as log:

                        device_output = []                                    # Empty list to append into

                        # Same process as before, we'll loop through each subject
                        for key in tqdm(list(data.keys())):

                              username = key.split('-')[0]                    # Isolate username from key naming convention

                              try:

                                    # Isolate participant dictionary
                                    subset = data[key]
                                    device_output.append(parse_device_info(subset))

                              except Exception as e:

                                    # Catch exceptions as they occur
                                    log.write(f"\nCaught {username} @ device parser: {e}\n\n")

                        # Stack participant device info into one DF
                        devices = pd.concat(device_output)

                        # Push to local CSV
                        devices.to_csv(f'./{target_path}/01-Aggregate/devices_{output_filename}.csv',
                                      index=False, encoding="utf-8-sig")

                  sleep(1)
                  print("\nAll responses + devices parsed\n")


if __name__ == "__main__":
      main()

