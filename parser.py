#!/bin/python3

"""
About this Script

These helper functions convert *long* participant responses (derived from a JSON file)
into *wide* particticipant responses, such that one row in a DataFrame represents a complete
ping. Several of these functions can be better optimized (stripping out leading/trailing quoation
marks is a fine example) ... this project is fully functional as of 11/16/2021

Ian Ferguson | Stanford University
"""

# ----------- Imports
import os, pathlib
import pandas as pd
import numpy as np
from tqdm import tqdm
from time import sleep


# ----------- Definitions

# ----- Global
def setup(PATH):
    """
    PATH => Relative path to project directory

    Runs before parsing
    Creates required output directories if they don't already exist
    """

    for output_path in ["00-Subjects", "01-Aggregate"]:

        # Subjects => Subject specific CSVs
        # Aggregate => Subject CSV, Device CSV, Parent errors JSON file

        if not os.path.exists(os.path.join(".", PATH, output_path)):

            print(f"Creating {output_path}...")

            # Create the output directory if it doesn't exist
            pathlib.Path(os.path.join(".", PATH, output_path)).mkdir(exist_ok=True, 
                                                                     parents=True)

            sleep(1.5)


def isolate_json_file(PATH):
    """
    PATH => Relative path to project directory

    You should have ONE JSON file in your project directory
    This function isolates it and returns:
        * The JSON itself
        * The isolated filename for later use
    """

    # Should be a list of length 1
    files = [x for x in os.listdir(os.path.join(".", PATH)) if ".json" in x]

    # Raise error if there are more than 1 JSON file
    if len(files) > 1:
        raise OSError(f"Your project directory should only have one JSON file ... check {PATH} again")

    # E.g., test_data.json => test_data
    filename = files[0].split('.json')[0]

    return os.path.join(".", PATH, files[0]), filename


def output(KEY, PINGS, ANSWERS, OUTPUT_DIR, KICKOUT):
    """
    KEY => Key from JSON file
    PINGS => Pandas DataFrame object
    ANSWERS => Pandas DataFrame object
    OUTPUT_DIR => Relative path to aggregates directory
    KICKOUT => Boolean, determiens if CSV will be saved

    Merges pings and answers dataframes
    Returns DataFrame object
    """

    # Isolate username
    KEY = KEY.split('-')[0]

    # Combine dataframes on ping identifier (e.g., modalStream1)
    composite_dataframe = PINGS.merge(ANSWERS, on="id")

    # Option to save locally or not
    if KICKOUT:
        output_name = os.path.join(f"{OUTPUT_DIR}/{KEY}.csv")

        # Avoid duplicates (possible with same username / different login IDs)
        if os.path.exists(output_name):
            output_name = os.path.join(f"{OUTPUT_DIR}/{KEY}_b.csv")

        composite_dataframe.to_csv(output_name, index=False, encoding="utf-8-sig")

    return composite_dataframe


# ----- Answers

def derive_answers(SUBSET, LOG, USER):
    """
    SUBSET => Reduced dictionary of subject information (pings/user/answers)
    LOG => Text file to log issues
    USER => Username, used in error log

    This function isolates participant respones and converts from long to wide
    Returns DataFrame object
    """

    def isolate_values(DF):
        """
        DF => Dataframe object

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

    # Isolated participant response dictionary
    answers = pd.DataFrame(SUBSET['answers'])

    try:

        # Create new "value" column with aggregated response
        answers['value'] = answers.apply(isolate_values, axis=1)

    except Exception as e:

        # Write to error log
        LOG.write(f"\nCaught @ {USER} + isolate_values: {e}\n\n")

    try:

        # Apply cleanup_values function (removes extra characters)
        answers['value'] = answers['value'].apply(lambda x: cleanup_values(x))

    except Exception as e:

        # Write to error log
        LOG.write(f"\nCaught @ {USER} + cleanup_values: {e}\n\n")

    answers = answers.drop_duplicates(subset="date", keep="first").reset_index(drop=True)

    answers["IX"] = answers.groupby("questionId", as_index=False).cumcount()

    # Drop extraneous columns
    answers.drop(columns=['data', 'preferNotToAnswer'], inplace=True)

    # Pivot long to wide
    answers = answers.pivot(index="pingId", columns="questionId", values="value").reset_index()

    # Rename Ping ID column (for merge with pings DF)
    answers.rename(columns={'pingId':'id'}, inplace=True)

    return answers


def cleanup_values(x):
    """
    x => Isolated value derived from lambda

    This function is applied via lambda, serialized per column
    """

    # Parse out parentheses
    temp = str(x).replace('(', '').replace(')', '')

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


def parse_nominations(DF):
    """
    DF => Dataframe object

    This function is named nominations ... e.g., Dean Baltiansky

    The following columns are parsed...
        * SU_Nom => 1,2,3
        * SU_Nom_None_Nom => 1,2,3
        * NSU_Rel => 1,2,3
        * NSU_Nom_None_Nom => 1,2,3
    """

    # Keys are existing columns, Values are new columns
    voi = {'SU_Nom': 'SU_Nom_{}',
           'SU_Nom_None_Nom': 'SU_Nom_None_Nom_{}',
           'NSU_Rel': 'NSU{}_Rel',
           'NSU_Nom_None_Nom': 'NSU{}_None_Rel'}

    for parent in list(voi.keys()):                                      

        # Not every participant has every variable ... this will standardize it
        if parent not in list(DF.columns):
            DF[parent] = [] * len(DF)
            continue

        for k in [1, 2, 3]:                                                 
            new_var = voi[parent].format(k)                                 # E.g., SU_Nom_1
            DF[new_var] = [''] * len(DF)                                    # Create empty column

        for ix, value in enumerate(DF[parent]):                       
            try:
                check_nan = np.isnan(value)                                 # If value is null we'll skip over it
                continue
            except:
                if str(value) == "None":                                    # Skip over "None" and "PNA" values
                    continue
                elif value == "PNA":
                    continue

            value = value.replace("\"", "\'").split("\',")                  # Replace double-quotes, split on comma b/w nominees

            for k in range(len(value)):                                     
                new_var = voi[parent].format(k+1)

                try:                           
                    new_val = value[k]                                      # Isolate nominee by list position
                except:
                    continue                                                # Skip over index error

                for char in ["[", "]"]:
                    new_val = new_val.replace(char, "")                     # Strip out square brackets

                new_val = new_val.strip()                                   # Remove leading / trailing space

                DF.loc[ix, new_var] = new_val                               # Push isolated nominee to DF

    for parent in list(voi.keys()):
        for k in [1,2,3]:
            new_var = voi[parent].format(k)

            # Run cleanup_values function again to strip out leading / trailing characters (for roster matching)
            DF[new_var] = DF[new_var].apply(lambda x: cleanup_values(x))

    return DF


def parse_race(DF):
    """
    DF => DataFrame object

    This function un-nests race responses
    Returns list of all responses marked True (may be more than one)
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
            race_vals = [k.split(',')[0] for k in temp.split('],') if "True" in k]
            
            # Strip out square brackets
            race_vals = [k.strip().replace('[', '').replace(']', '') for k in race_vals]

            return race_vals

        except:

            # In the case of missing data
            return None

    try:

        # Make sure Race column is present
        check = list(DF['Race'])

        # Apply isolate_race_value helper function
        DF['Race'] = DF['Race'].apply(lambda x: isolate_race_value(x))

    except:

        # If key doesn't exist, create empty column
        DF['Race'] = [] * len(DF)

    return DF


# ----- Pings

def derive_pings(SUBSET, KEY):
    """
    SUBSET => Reduced dictionary containing 
    KEY => Key from the master JSON

    This function isolates ping data from the participant's dictionary
    Returns wide DataFrame object with select columns
    """

    pings = pd.DataFrame(SUBSET['pings'])                                   # Convert JSON to DataFrame
    pings['username'] = KEY.split('-')[0]                                   # Add username column
    
    login_node = KEY.split('-')[1:]
    login_node = "".join(login_node)

    pings['login-node'] = login_node

    return pings.loc[:, ['username', 'login-node', 'streamName', 'startTime', 
                        'notificationTime', 'endTime', 'id', 'tzOffset']]


# ----- Concat
def agg_drop_duplicates(DF):
    """
    
    """

    users = list(DF['username'].unique())

    keepers = []

    for user in tqdm(users):
        temp = DF[DF['username'] == user].reset_index(drop=True)
        temp = temp.drop_duplicates(subset="id", keep="first").reset_index(drop=True)
        keepers.append(temp)

    return pd.concat(keepers)


# ----- Run
def parse_responses(KEY, SUBSET, LOG, OUTPUT_DIR, KICKOUT):
    """
    KEY => Key from the master data dictionary
    SUBSET => Reduced dictionary of participant-only data
    LOG => Text file to store errors and exceptions
    OUTPUT_DIR => Relative path to output directory
    KICKOUT => Boolean, if True a local CSV is saved

    This function wraps everything defined above
    Returns a clean DataFrame object
    """

    username = KEY.split('-')[0]                                            # Isolate username

    try:
        answers = derive_answers(SUBSET=SUBSET, LOG=LOG, USER=username)     # Create answers DataFrame
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + derive_answers: {e}\n\n")

    try:
        answers = parse_race(answers)                                       # Isolate race responses
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + parse_race: {e}\n\n")

    try:
        answers = parse_nominations(answers)                                # Isolate nomination responses
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + parse_nominations: {e}\n\n")

    try:
        pings = derive_pings(SUBSET=SUBSET, KEY=KEY)                        # Create pings DataFrame
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + derive_pings: {e}\n\n")

    # Isolate a few device parameters to include in pings CSV
    # The exhaustive device info is in another CSV in the same directory
    devices = pd.DataFrame(SUBSET['user']['installation']['device'], index=[0])
    devices['username'] = username
    pings = pings.merge(devices, on="username")

    return output(KEY, pings, answers, OUTPUT_DIR, KICKOUT)