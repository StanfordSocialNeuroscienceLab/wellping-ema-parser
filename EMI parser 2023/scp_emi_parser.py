#!/bin/python3
from datetime import datetime
import os, pathlib, json, sys, tarfile
import shutil
from tqdm import tqdm
import pandas as pd
import numpy as np
from time import sleep


##########


class EMI_Parser:
      """
      We want to execute the following actions on the SCP EMA data:
            * Parse subject-level pings
            * Aggregate into a master CSV
            * Gunzip all files together
            * Delete directory tree

      We'll then download the resulting files for the end user
      """

      def __init__(self, path_to_file: os.path):

            self.root = pathlib.Path(path_to_file).parents[0]
            self.filepath = path_to_file

            ###

            self.filename = path_to_file.split("/")[-1]

            self.output_path = os.path.join(
                        self.root,
                        "OUTPUT")

            ###

            output = self._output_directories()
            self.subject_output = output[0]
            self.aggregate_output = output[1]


      def _output_directories(self) -> list:
            """
            Creates relevant subdirectories and saves
            to list for object assignment
            """

            output = []

            for subdir in ["Subjects", "SCP-EMA_Output"]:

                  temp = os.path.join(self.output_path, subdir)
                  pathlib.Path(temp).mkdir(exist_ok=True, parents=True)
                  output.append(temp)

            return output


      def generate_duplicate_responses(self):
            """
            Creates a JSON file of subjects with duplicate responses
            """

            # Read in subject data JSON as dictionary
            with open(self.filepath) as incoming:
                  data = json.load(incoming)

            # List of keys from JSON
            keys = list(data.keys())

            # Unique subject IDs
            sub_ids = set([x.split('-')[0] for x in keys])

            # Empty dictionary to append into
            output_dict = {}

            print("Identifying duplicate subject responses...")

            #####

            for sub in tqdm(sub_ids):
                  # Number of responses from single sub
                  instances = [x for x in keys if sub in x]

                  # Add to output dict if multiples exist
                  if len(instances) > 1:
                        output_dict[sub] = {}
                        output_dict[sub]['count'] = len(instances)
                        output_dict[sub]['keys'] = instances

            print("Saving response-duplicates JSON file...")

            #####

            # Push to local JSON file
            with open(os.path.join(self.aggregate_output, "response-duplicates.json"), "w") as outgoing:
                  json.dump(
                        output_dict,
                        outgoing,
                        indent=4)


      #####


      def derive_pings(self, SUBSET: dict, KEY: str) -> pd.DataFrame:
            """
            * SUBSET: Reduced dictionary containing
            * KEY: Key from the master JSON

            This function isolates ping data from the participant's dictionary
            Returns wide DataFrame object with select columns
            """

            # JSON -> DataFrame
            pings = pd.DataFrame(SUBSET['pings'])

            # Add username column
            pings['username'] = KEY.split('-')[0]

            login_node = KEY.split('-')[1:]
            login_node = "".join(login_node)
            pings['login-node'] = login_node

            keeper_variables = ['username', 'login-node', 'streamName', 'startTime',
                                'notificationTime', 'endTime', 'id', 'tzOffset']

            return pings.loc[:, keeper_variables]


      #####


      def derive_answers(self, SUBSET: dict, LOG, USER: str) -> pd.DataFrame:
            """
            * SUBSET: Reduced dictionary of subject information (pings/user/answers)
            * LOG: Text file to log issues
            * USER: Username, used in error log

            This function isolates participant respones and converts from long to wide
            Returns DataFrame object
            """

            def isolate_values(DF: pd.DataFrame):
                  """
                  While data is still "long", we'll isolate the participant response
                  """

                  if DF['preferNotToAnswer']:
                        return "PNA"

                  try:
                        # Raw data is optimized for dictionary expresson, we'll save the values
                        temp = dict(DF['data']).values()
                        return list(temp)

                  except:
                        # NOTE: Consider returning empty string instead
                        return None

            #####

            # Isolated participant response dictionary
            answers = pd.DataFrame(SUBSET['answers'])

            try:
                  # Create new "value" column with aggregated response
                  answers['value'] = answers.apply(isolate_values, axis=1)

            except Exception as e:
                  # Write to error log
                  LOG.write(f"\nCaught @ {USER} + isolate_values: {e}\n\n")

            ###

            try:
                  # Apply cleanup_values function (removes extra characters)
                  answers['value'] = answers['value'].apply(lambda x: self.cleanup_values(x))

            except Exception as e:
                  # Write to error log
                  LOG.write(f"\nCaught @ {USER} + cleanup_values: {e}\n\n")

            ###

            answers = answers.drop_duplicates(subset="date", keep="first").reset_index(drop=True)
            answers["IX"] = answers.groupby("questionId", as_index=False).cumcount()

            # Drop extraneous columns
            answers.drop(columns=['data', 'preferNotToAnswer'], inplace=True)

            # Pivot long to wide
            answers = answers.pivot(index="pingId", columns="questionId", values="value").reset_index()

            # Rename Ping ID column (for merge with pings DF)
            answers.rename(columns={'pingId':'id'}, inplace=True)

            return answers


      def cleanup_values(self, x) -> str:
            """
            * x: Isolated value derived from lambda

            This function is applied via lambda, serialized per column
            """

            # Parse out parentheses
            #temp = str(x).replace('(', '').replace(')', '')
            temp = str(x)

            """
            The conditional statements below will strip out square brackets
            and leading / trailing parentheses
            Yields a clean value to work with in the resulting dataframe
            """

            if temp[0] == "[":
                  temp = temp[1:]

            if temp[-1] == "]":
                  temp = temp[:-1]

            if temp[0] == "\'":
                  temp = temp[1:]
            elif temp[0] == "\"":
                  temp = temp[1:]

            if temp[-1] == "\'":
                  temp = temp[:-1]
            elif temp[-1] == "\"":
                  temp = temp[:-1]

            return temp


      def parse_nominations(self, DF: pd.DataFrame):
            """
            This function is named nominations ... e.g., Dean Baltiansky

            The following columns are parsed...
                  * SU_Nom => 1,2,3
                  * SU_Nom_None_Nom => 1,2,3
                  * NSU_Rel => 1,2,3
                  * NSU_Nom_None_Nom => 1,2,3
            """

            # Keys are existing columns, Values are new columns
            voi = {
                  'SU_Nom': 'SU_Nom_{}',
                  'SU_Nom_None_Nom': 'SU_Nom_None_Nom_{}',
                  'SU_Nom_None_Digital_Nom':'SU_Nom_None_Digital_Nom_{}',
                  'SU_Digital_Nom':'SU_Digital_Nom_{}',
                  'SU_Digital_Nom_None_In_Person': 'SU_Digital_Nom_None_In_Person_{}',
                  'SU_Nom_None_Digital_Nom_None_In_Person':'SU_Nom_None_Digital_Nom_None_In_Person_{}',
                  'NSU_Rel': 'NSU{}_Rel',
                  'NSU_Nom_None_Nom': 'NSU{}_None_Rel'
            }

            #####

            for parent in list(voi.keys()):

                  try:
                  # Not every participant has every variable ... this will standardize it
                      if parent not in list(DF.columns):
                            DF[parent] = [] * len(DF)
                            continue

                      #####

                      for k in [1, 2, 3, 4, 5, 6]:
                            # E.g., SU_Nom_1
                            new_var = voi[parent].format(k)

                            # Create empty column
                            DF[new_var] = [''] * len(DF)


                  #####

                      for ix, value in enumerate(DF[parent]):

                            try:
                                # If value is null we'll skip over it
                                  check_nan = np.isnan(value)
                                  continue

                            except:
                                # Skip over "None" and "PNA" values
                                  if str(value) == "None":
                                        continue
                                  elif value == "PNA":
                                        continue

                            ###

                            # Replace double-quotes, split on comma b/w nominees
                            value = value.replace("\"", "\'").split("\',")

                            ###

                            for k in range(len(value)):
                                  new_var = voi[parent].format(k+1)
                                  try:
                                        # Isolate nominee by list position
                                        new_val = value[k]

                                  except IndexError:
                                        # Skip over index error
                                        continue

                            ###

                                  for char in ["[", "]"]:

                                        # Strip out square brackets
                                        new_val = new_val.replace(char, "")

                                  # Remove leading / trailing space
                                  new_val = new_val.strip()

                                  # Push isolated nominee to DF
                                  #if parent == "SU_Nom_None_Digital_Nom":
                                  #      print(parent)
                                  #      print(new_val)
                                  #      print(new_var)
                                  DF.loc[ix, new_var] = new_val


                #####
                  except BaseException as exception:
                    print("Ignored exception: " + type(exception).__name__)
                    continue


            for parent in list(voi.keys()):
                  for k in [1,2,3,4,5,6]:
                        new_var = voi[parent].format(k)

                        # Run cleanup_values function again to strip out
                        # leading / trailing characters (for roster matching)
                        DF[new_var] = DF[new_var].apply(lambda x: self.cleanup_values(x))

            return DF


      def parse_race(self, DF: pd.DataFrame):
            """
            * Un-nests race responses
            * Returns list of all responses marked True (may be more than one)
            * RP update (10/28/2022): make the function parse other variables as well
            """

            def isolate_race_value(x):
                  """
                  x => Isolated value derived from lambda
                  This function will be applied via lambda function
                  Returns list of True values
                  """

                  try:
                        # Strip out all quotes
                        temp = x.replace("\"", "").replace("\'", "")

                        # Split on category and isolate responses that were marked true
                        race_vals = [k.split(',')[0]
                                    for k in temp.split('],') if "True" in k]

                        # Strip out square brackets
                        race_vals = [k.strip().replace('[', '').replace(']', '')
                                    for k in race_vals]

                        return race_vals

                  except:
                        # In the case of missing data
                        return None

            #####

            vars_to_update = ['Race', 'socialRiskTaking', 'socMediaPlatforms']

            for var_name in vars_to_update:

                  try:
                        # Make sure Race column is present
                        check = list(DF[var_name])

                        # Apply isolate_race_value helper function
                        DF[var_name] = DF[var_name].apply(lambda x: isolate_race_value(x))

                  except:
                        # If key doesn't exist, create empty column
                        DF[var_name] = [] * len(DF)

            return DF

      def remove_brackets(self, DF: pd.DataFrame): 
            """
            * Removes brackets from variable
            * RP update (10/28/2022): make the function parse other variables as well
            """

            vars_to_update = ['SU_Most_Meaningful','ladderUS']

            for var_name in vars_to_update:

                  try:
                        # Make sure column is present
                        check = list(DF[var_name])

                        # remove bracket
                        DF[var_name] = DF[var_name].str.replace(r'[][]', '', regex=True)

                  except:
                        # If key doesn't exist, create empty column
                        DF[var_name] = [] * len(DF)

            return DF


      #####


      def agg_drop_duplicates(self, DF: pd.DataFrame) -> pd.DataFrame:
            """
            NOTE: This helper is functional but not in use
            """

            users = list(DF['username'].unique())
            keepers = []

            #####

            for user in tqdm(users):
                  temp = DF[DF['username'] == user].reset_index(drop=True)
                  temp = temp.drop_duplicates(subset="id", keep="first").reset_index(drop=True)
                  keepers.append(temp)

            return pd.concat(keepers)


      #####


      def parse_device_info(self, SUBSET: dict, KEY: str):
            """
            * SUBSET: Particpant's reduced JSON file (as Python dictionary)
            * KEY: Key from the JSON data dictionary

            This function flattens user device info into a single-row
            DataFrame object. This is returned and stacked with others
            in the main function

            Returns DataFrame object
            """

            # Isolate device information from JSON
            devices = SUBSET['user']

            # Pull in username from data dictionary
            username = devices['username']

            # Isolate subject login time from data key
            login_time = KEY.split('-')[-1]

            #####

            # Parent DF to merge into
            master = pd.DataFrame({'username':username}, index=[0])

            for key in ['device', 'app']:

                  # Isloate sub-keys from dictionary
                  temp = devices['installation'][key]

                  # Flatten wide to long
                  temp_frame = pd.DataFrame(temp, index=[0])

                  # Isolate username
                  temp_frame['username'] = username

                  # JavaScript derived login time
                  temp_frame['login_time'] = login_time

                  # Merge with parent DF on username
                  master = master.merge(temp_frame, on='username')

            return master


      #####


      def parse_responses(self, KEY: str, SUBSET: dict, LOG,
                          OUTPUT_DIR: os.path, KICKOUT: bool) -> pd.DataFrame:
            """
            * KEY: Key from the master data dictionary
            * SUBSET: Reduced dictionary of participant-only data
            * LOG: Text file to store errors and exceptions
            * OUTPUT_DIR: Relative path to output directory
            * KICKOUT: Boolean, if True a local CSV is saved
            """

            # Isolate username
            username = KEY.split('-')[0]

            try:
                  # Create answers DataFrame
                  answers = self.derive_answers(
                        SUBSET=SUBSET,
                        LOG=LOG,
                        USER=username)

            except Exception as e:
                  LOG.write(f"\nCaught @ {username} + derive_answers: {e}\n\n")

            ###

            try:
                  # Isolate race responses
                  answers = self.parse_race(answers)
                  ansers = self.remove_brackets(answers)

            except Exception as e:
                  LOG.write(f"\nCaught @ {username} + parse_race: {e}\n\n")

            ###

            try:
                  # Isolate nomination responses
                  answers = self.parse_nominations(answers)

            except Exception as e:
                  LOG.write(f"\nCaught @ {username} + parse_nominations: {e}\n\n")

            ###

            try:
                  # Create pings DataFrame
                  pings = self.derive_pings(
                        SUBSET=SUBSET,
                        KEY=KEY)

            except Exception as e:
                  LOG.write(f"\nCaught @ {username} + derive_pings: {e}\n\n")

            ###

            # Isolate a few device parameters to include in pings CSV
            # The exhaustive device info is in another CSV in the same directory
            devices = pd.DataFrame(SUBSET['user']['installation']['device'], index=[0])
            devices['username'] = username
            pings = pings.merge(devices, on="username")

            return self.output(KEY, pings, answers, OUTPUT_DIR, KICKOUT)



      def output(self, KEY: str, PINGS: pd.DataFrame,
                ANSWERS: pd.DataFrame, OUTPUT_DIR: os.path, KICKOUT: bool):
            """
            Merges pings and answers dataframes

            * KEY: Key from JSON file
            * PINGS: Pandas DataFrame object
            * ANSWERS: Pandas DataFrame object
            * OUTPUT_DIR: Relative path to aggregates directory
            * KICKOUT: Boolean, determiens if CSV will be saved
            """

            # Isolate username
            KEY = KEY.split('-')[0]

            # Combine dataframes on ping identifier (e.g., modalStream1)
            composite_dataframe = PINGS.merge(ANSWERS, on="id")

            #####

            # Option to save locally or not
            if KICKOUT:
                  output_name = os.path.join(f"{OUTPUT_DIR}/{KEY}.csv")

                  # Avoid duplicates (possible with same username / different login IDs)
                  if os.path.exists(output_name):
                        output_name = os.path.join(f"{OUTPUT_DIR}/{KEY}_b.csv")

                  composite_dataframe.to_csv(output_name, index=False, encoding="utf-8-sig")

            return composite_dataframe



      def run_parser(self):
            """
            Wraps all parsing helper functions

            * Parses device and response data
            * Aggregates response data in a single CSV
            """

            target_path = self.output_path
            sub_data, output_filename = self.filename, self.filename.split('.json')[0]

            # These output directories will hold parsed data
            subject_output_directory = self.subject_output
            aggregate_output_directory = self.aggregate_output

            self.generate_duplicate_responses()

            #####

            with open(self.filepath) as incoming:

                  print(f"\nParsing {self.filepath}")

                  #####

                  with open(f"{target_path}/{output_filename}.txt", "w") as log:

                        # Read JSON as Python dictionary
                        data = json.load(incoming)

                        # Empty list to append subject data into
                        keepers = []

                        # Empty dictionary to append sparse data into
                        parent_errors = {}

                        print("\nParsing participant data...")
                        sleep(1)

                        # Key == Subject and login ID (we'll separate these later)
                        for key in tqdm(list(data.keys())):

                              # Reduced data for one participant
                              subset = data[key]

                              # If participant completed no pings, push them to parent dict
                              if len(subset['answers']) == 0:
                                    parent_errors[key] = subset
                                    continue

                              try:
                                    # Run parse_responses function to isolate participant data
                                    parsed_data = self.parse_responses(
                                          key,
                                          subset,
                                          log,
                                          subject_output_directory,
                                          True)

                              except Exception as e:
                                    # Catch exceptions as they occur
                                    log.write(f"\nCaught @ {key.split('-')[0]}: {e}\n\n")
                                    continue

                              # Add participant DF to keepers list
                              keepers.append(parsed_data)

                        sleep(1)
                        print("Aggregating participant data...")

                        try:
                              # Stack all DFs into one
                              aggregate = pd.concat(keepers)

                              # Push to local CSV
                              aggregate.to_csv(f'{self.aggregate_output}/pings_{output_filename}.csv',
                                                index=False, encoding="utf-8-sig")

                        except Exception as e:
                              # Something has gone wrong here and you have no participant data ... check the log
                              print(f"{e}")
                              print("No objects to concatenate...")
                              sys.exit(1)

                        print("Saving parent errors...")

                        # Push parent errors (no pings) to local JSON
                        with open(f'{self.aggregate_output}/parent-errors.json', 'w') as outgoing:
                              json.dump(parent_errors, outgoing, indent=4)

                        print("\nParsing device information...")

                        # I/O new text file for device parsing errors
                        with open(f"{self.aggregate_output}/device-error-log.txt", 'w') as log:

                              device_output = []                                    # Empty list to append into

                              # Same process as before, we'll loop through each subject
                              for key in tqdm(list(data.keys())):

                                    username = key.split('-')[0]                    # Isolate username from key naming convention

                                    try:
                                          # Isolate participant dictionary
                                          subset = data[key]
                                          device_output.append(self.parse_device_info(subset, key))

                                    except Exception as e:
                                          # Catch exceptions as they occur
                                          log.write(f"\nCaught {username} @ device parser: {e}\n\n")

                                    # Stack participant device info into one DF
                                    devices = pd.concat(device_output)

                                    # Push to local CSV
                                    devices.to_csv(f'{self.aggregate_output}/devices_{output_filename}.csv',
                                                index=False, encoding="utf-8-sig")

                              sleep(1)
                              print("\nAll responses + devices parsed\n")


      def gunzip(self):
            """
            Helper function to create gunzipped directory
            """

            filename = datetime.now().strftime("%b_%d_%Y")
            tarfile_name = f"{self.output_path}/SCP_EMA_Responses.tar.gz"

            with tarfile.open(tarfile_name, "w:gz") as tar:
                  tar.add(
                        self.aggregate_output,
                        arcname=f"SCP_EMA_Responses_{filename}"
                  )



      def run_and_gun(self):
            """
            Wrapper to run and compress output
            in one function
            """

            print("\n== Parsing ==")
            self.run_parser()
            sleep(2)

            print("== Zipping ==")
            self.gunzip()
