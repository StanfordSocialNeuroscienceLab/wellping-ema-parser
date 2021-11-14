#!/bin/python3

"""
About this Script

"""

"""
Running To-Do

* Check in w/Sam regarding interaction variables
* Parse out device information
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
    
    """

    for output_path in ["00-Subjects", "01-Aggregate"]:
        if not os.path.exists(os.path.join(".", PATH, output_path)):

            print(f"Creating {output_path}...")

            pathlib.Path(os.path.join(".", PATH, output_path)).mkdir(exist_ok=True, 
                                                                     parents=True)

            sleep(1)


def isolate_json_file(PATH):
    """
    
    """

    files = [x for x in os.listdir(os.path.join(".", PATH)) if ".json" in x]

    if len(files) > 1:
        raise OSError(f"Your project directory should only have one JSON file ... check {PATH} again")

    filename = files[0].split('.json')[0]

    return os.path.join(".", PATH, files[0]), filename


def output(KEY, PINGS, ANSWERS, OUTPUT_DIR):
    """
    
    """

    KEY = KEY.split('-')[0]

    composite_dataframe = PINGS.merge(ANSWERS, on="id")
    composite_dataframe.to_csv(os.path.join(f"{OUTPUT_DIR}/{KEY}.csv"),
                               index=False, 
                               encoding="utf-8-sig")

    return composite_dataframe


# ----- Answers

def derive_answers(SUBSET, LOG, USER):
    """
    
    """

    def isolate_values(DF):
        if DF['preferNotToAnswer']:
            return "PNA"

        try:
            temp = dict(DF['data']).values()
            return list(temp)
        except:
            return None

    def cleanup_values(x):
        temp = str(x).replace('(', '').replace(')', '')

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

    answers = pd.DataFrame(SUBSET['answers'])

    try:
        answers['value'] = answers.apply(isolate_values, axis=1)
    except Exception as e:
        LOG.write(f"\nCaught @ {USER} + isolate_values: {e}\n\n")

    try:
        answers['value'] = answers['value'].apply(lambda x: cleanup_values(x))
    except Exception as e:
        LOG.write(f"\nCaught @ {USER} + cleanup_values: {e}\n\n")

    answers.drop(columns=['data', 'preferNotToAnswer'], inplace=True)
    answers = answers.pivot(index="pingId", columns="questionId", values="value").reset_index()
    answers.rename(columns={'pingId':'id'}, inplace=True)

    return answers


def parse_nominations(DF):
    """
    DF =>

    This function is named nominations ... e.g., Dean Baltiansky

    The following columns are parsed...
        * SU_Nom => 1,2,3
        * SU_Nom_None_Nom => 1,2,3
        * NSU_Rel => 1,2,3
        * NSU_Nom_None_Nom => 1,2,3

    Functions inplace, no return
    """

    voi = {'SU_Nom': 'SU_Nom_{}',
           'SU_Nom_None_Nom': 'SU_Nom_None_Nom_{}',
           'NSU_Rel': 'NSU{}_Rel',
           'NSU_Nom_None_Nom': 'NSU{}_None_Rel'}

    for parent in voi:
        for k in [1, 2, 3]:
            new_var = parent.format(k)
            DF[new_var] = [None] * len(DF)

        for ix, value in enumerate(DF[parent]):
            try:
                check_nan = np.isnan(value)
                continue
            except:
                if value is None:
                    continue
                elif value == "PNA":
                    continue

            value = value.replace("\"", "").split("\',")

            for k in range(len(value)):
                new_var = parent.format(k+1)
                new_val = value[k]

                for char in ["[", "]"]:
                    new_val = new_val.replace(char, "")

                new_val = new_val.strip().replace("\'", "")

                DF.loc[ix, new_var] = new_val


def parse_interaction_types(DF, LOG, USER):
    """
    DF =>
    LOG =>
    USER =>

    This function is for interaction types ... e.g., friend, teammate
    We'll isolate responses marked True

    The following columns are parsed...
        * NSU{[1,2,3]}_interaction
        * SU{[1,2,3]}_interaction

    Functions inplace, no return
    """

    voi = ["SU{}_interaction", "NSU{}_interaction"]

    for variable in voi:
        for k in [1,2,3]:
            temp_var = variable.format(k)

            try:
                temp = DF.loc[:, temp_var]
            except Exception as e:
                temp = DF
                LOG.write(f"Caught @ {USER} + interaction_types: {e}\n\n")

            for idx, response in enumerate(temp):
                real_values = []

                try:
                    clean = response.strip("[").strip("]").split("]")
                except:
                    continue

                for item in clean:
                    if "True" in item:
                        item = item.strip(",").strip("[").split(",")
                        real_values.append(item[0].strip("["))

                try:
                    DF.loc[idx, temp_var] = str(real_values)
                except:
                    DF.loc[idx, temp_var] = "FAILED TO PARSE"


def parse_race(DF):
    """
    
    """

    def isolate_race_value(x):
        try:
            temp = x.replace("\"", "").replace("\'", "")
            race_vals = [k.split(',')[0] for k in temp.split('],') if "True" in k]
            race_vals = [k.strip().replace('[', '').replace(']', '') for k in race_vals]

            return race_vals

        except:
            return None

    try:
        check = list(DF['Race'])
        DF['Race'] = DF['Race'].apply(lambda x: isolate_race_value(x))
    except:
        DF['Race'] = [None] * len(DF)


# ----- Pings

def derive_pings(SUBSET, KEY):
    """
    
    """

    pings = pd.DataFrame(SUBSET['pings'])
    pings['username'] = KEY.split('-')[0]

    return pings.loc[:, ['username', 'streamName', 'startTime', 
                        'notificationTime', 'endTime', 'id', 'tzOffset']]


# ----- Run
def parse_responses(KEY, SUBSET, LOG, OUTPUT_DIR):
    """
    
    """

    username = KEY.split('-')[0]

    try:
        answers = derive_answers(SUBSET=SUBSET, LOG=LOG, USER=username)
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + derive_answers: {e}\n\n")

    try:
        parse_race(answers)
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + parse_race: {e}\n\n")

    try:
        parse_nominations(answers)
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + parse_nominations: {e}\n\n")

    # ---- NOTE: Interaction variables don't seem to be included in this iteration of the app

    """
    try:
        parse_interaction_types(answers, LOG, username)
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + interaction_types: {e}\n\n")
    """

    try:
        pings = derive_pings(SUBSET=SUBSET, KEY=KEY)
    except Exception as e:
        LOG.write(f"\nCaught @ {username} + derive_pings: {e}\n\n")

    return output(KEY, pings, answers, OUTPUT_DIR)
