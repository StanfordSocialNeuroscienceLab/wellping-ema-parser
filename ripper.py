#!/bin/python3

"""

"""

# ----- Imports
import os, sys, json
from time import sleep
from tqdm import tqdm
import pandas as pd
from parser import setup, isolate_json_file, parse_responses


# ----- Run Script
def main():
      target_path = sys.argv[1]
      setup(target_path)
      sub_data, output_filename = isolate_json_file(target_path)

      subject_output_directory = os.path.join(".", target_path, "00-Subjects")
      aggregate_output_directory = os.path.join(".", target_path, "01-Aggregate")

      with open(sub_data) as incoming:
            with open(f"./{target_path}/{output_filename}.txt", "w") as log:
                  data = json.load(incoming)

                  keepers = []
                  parent_errors = {}

                  print("\nParsing participant data...\n")
                  sleep(1)

                  for key in tqdm(list(data.keys())):
                        subset = data[key]

                        if len(subset['answers']) == 0:
                              parent_errors[key] = subset
                              continue
                        
                        try:
                              parsed_data = parse_responses(key, subset, log, subject_output_directory)
                        except Exception as e:
                              log.write(f"\nCaught @ {key.split('-')[0]}: {e}\n\n")
                              continue

                        keepers.append(parsed_data)

                  sleep(1)
                  print("\nAggregating participant data...\n")

                  try:
                        aggregate = pd.concat(keepers)
                        aggregate.to_csv(f'./{target_path}/01-Aggregate/{output_filename}.csv',
                                          index=False, encoding="utf-8-sig")
                  except:
                        print("\nNo objects to concatenate...\n")

                  print("\nSaving parent errors...\n")

                  with open(f'./{target_path}/01-Aggregate/parent-errors.json', 'w') as outgoing:
                        json.dump(parent_errors, outgoing, indent=4)


if __name__ == "__main__":
      main()

