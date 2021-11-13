#!/bin/python3

"""
About this Script

"""

# ----------- Imports
import os, pathlib
import pandas as pd
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

    try:
        parse_race(answers)
    except Exception as e:
        LOG.write(f"\nCaught @ {USER} + parse_race: {e}\n\n")

    return answers


def parse_nominations(DF):
    """
    
    """

    pass


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
        LOG.write(f"\nCaught @ {username}: {e}\n\n")

    try:
        pings = derive_pings(SUBSET=SUBSET, KEY=KEY)
    except Exception as e:
        LOG.write(f"\nCaught @ {username}: {e}\n\n")

    return output(KEY, pings, answers, OUTPUT_DIR)
