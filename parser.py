#!/bin/python3

"""
WellPing EMA Parser - Parsing Script

About this script
    * Individual EMA response files are fed into this program
    * Data is pivoted from long (JSON) to wide (DataFrame)
    * Values are cleaned (e.g., converted to string/int values)
    * Multiple-choice answers are parsed and saved as lists

An individual participant's CSV to saved to the output directory
Ian Richard Ferguson | Stanford University
"""


# ---- Imports
import os
from time import sleep
import pandas as pd
from tqdm import tqdm


# ---- Answers
def clean_answers(DF):
    """
    For answers data in long format
    """

    DF['preferNotToAnswer'] = DF['preferNotToAnswer'].astype(str)               # Convert PNA column to string

    for ix, val in enumerate(DF["data"]):
        try:
            temp = val.values()                                                 # Isolate participant responses, if not NA
            DF.loc[ix, "data"] = str(temp)
        except:
            DF.loc[ix, "data"] = val

        if DF['preferNotToAnswer'][ix] == "True":
            DF['data'][ix] = "PNA"                                              # Fill PNA values appropriately

    DF['data'] = DF['data'].map(lambda x: str(x)[13:-2])                        # Strip dictionary brackets

    return DF


def PNA(DF):
    """
    Replaces data values with "PNA" when applicable
    """
    for ix, val in enumerate(DF['preferNotToAnswer']):                          # Another PNA iteration...
        if str(val) == "True":                                                  # In case the cleanAnswers() v doesn't work lol
            DF["data"][ix] = "PNA"


def flatten_answers(DF):
    """
    Converts DF from long to wide format
    """

    # Unique timestamps ONLY - this skips over data duplication issue (date 7/9/2021)
    DF = DF.drop_duplicates(subset="date", keep="first").reset_index(drop=True)
    DF["IX"] = DF.groupby("questionId", as_index=False).cumcount()

    return DF.pivot(index="pingId", columns="questionId", values="data")


def split_nominations(DF):
    """
    If applicable, splits SU_Nom column into three distinct columns
    """

    # Variable names ready to be formatted
    new_vars = {"SU_Nom": "SU_Nom_{}",
                "SU_Nom_None_Nom": "SU_No_Nom_{}",
                "NSU_Rel": "NSU{}_Rel",
                "NSU_Nom_None_Nom":"NSU{}_None_Rel"}

    # Loop through new variables and default to NA
    for var in list(new_vars.keys()):
        for k in [1,2,3]:
            new_var = new_vars[var].format(k)                                   # E.g., SU_Nom_1
            DF[new_var] = "NA"                                                  # Fill with NA values

    # Split responses where applicable
    for variable in list(new_vars.keys()):
        for index, response in enumerate(DF.loc[:, variable]):
            try:
                temp = response.split(",")                                      # Vectorize subject responses
            except:
                temp = response

            for k in [0,1,2]:
                var_name = new_vars[variable].format(k+1)                       # 0 becomes 1, 1 becomes 2, etc

                try:                                                            # Strip the brackets off of new strings
                    if k == 0:
                        keep = temp[k].strip("[").strip()
                    elif k == 2:
                        keep = temp[k].strip("]").strip()
                    else:
                        keep = temp[k].strip()

                    DF.loc[index, var_name] = keep

                except:
                    continue


def split_NSU(DF):
    # Default to NA values
    for k in [1,2,3]:
        var_nom = f"SU_Nom_{k}"
        var_no_nom = f"SU_No_Nom_{k}"

        for m in [var_nom, var_no_nom]:
            DF[m] = "NA"

    for ix, response in enumerate(DF.loc[:, "SU_Nom"]):
        try:
            temp = response.split(",")
        except:
            temp = response

        for k in [0,1,2]:
            var = f"SU_Nom_{k+1}"

            try:
                if k == 0:
                    DF.loc[ix, var] = temp[k].strip("[").strip()
                elif k == 2:
                    DF.loc[ix, var] = temp[k].strip("]").strip()
                else:
                    DF.loc[ix, var] = temp[k].strip()
            except:
                continue

    for ix, response in enumerate(DF.loc[:, "SU_Nom_None_Nom"]):
        try:
            temp = response.split(",")
        except:
            temp = response

        for k in [0,1,2]:
            var = f"SU_No_Nom_{k+1}"

            try:
                if k == 0:
                    DF.loc[ix, var] = temp[k].strip("[").strip()
                elif k == 2:
                    DF.loc[ix, var] = temp[k].strip("]").strip()
                else:
                    DF.loc[ix, var] = temp[k].strip()
            except:
                continue

                
def define_interactions(DF, LOG):
    """
    Loops through "interaction" variables and finds "True" values
    Replaces values in DF with values marked "True" by participants
    """

    target_vars = ['NSU1_interaction', 'NSU2_interaction', 'NSU3_interaction',
                   'SU1_interaction', 'SU2_interaction', 'SU3_interaction']

    for var in target_vars:
        try:
            temp = DF.loc[:, "questionId" == var]
        except Exception as e:
            temp = DF
            LOG.write("{} at {}...\n".format(type(e), var))                     # KeyError

        for idx, response in enumerate(temp):
            real_vals = []

            try:
                clean = response.strip("[").strip("]").split("]")
            except:
                continue

            for item in clean:
                if "True" in item:
                    item = item.strip(",").strip("[").split(",")
                    real_vals.append(item[0].strip("["))

            try:
                DF.loc[idx, var] = str(real_vals)
            except:
                DF.loc[idx, var] = "NA"


def split_race(DF):
    """
    Loops through possible race answers
    Replaces DF value with race marked "True"
    """

    for idx, response in enumerate(DF['Race']):
        real_vals = []                                                          # Empty list to append into

        try:
            clean = response.strip("[").strip("]").split("]")                   # Strip outside brackets
        except:
            continue

        for item in clean:
            if "True" in item:
                item = item.strip(",").strip("[").split(",")                    # Strip inside brackets
                real_vals.append(item[0].strip("["))                            # Add to list

        try:
            DF.loc[idx, 'Race'] = str(real_vals)                                # Replace Race column with True vals
        except:
            DF.loc[idx, 'Race'] = "NA"


def cleanup(DF):
    """
    Strips out extraneous characters for every item in each column
    """

    for var in DF.columns:
        for char in ['[',']']:
            DF[var] = DF[var].map(lambda x: str(x).replace(char, ""))


# ---- Pings
def add_username(DF, IX):
    """
    Adds username column + reorders columns appropriately
    """

    DF["userName"] = IX
    cleanColumns = ["streamName", "userName", "startTime", "notificationTime", "endTime", "id", "tzOffset"]
    DF = DF[cleanColumns]
    return DF


# ---- Wrapper
def setup(here):
    """
    Check to see if output directories exist
    If they don't - create them!
    """

    for repo in ["89_Participant-Files", "99_Composite-CSV"]:
        if not os.path.exists(os.path.join(here,repo)):
            print("Creating {} directory...".format(repo))
            os.mkdir(os.path.join(here, repo))
            sleep(1)


def iso_JSON(DIR):
    """
    Finds JSON file in the target directory and isolates it
    """

    for file in os.listdir(DIR):
        if file.endswith(".json"):
            return file
        else:
            continue


def output(filename="Composite-Responses"):
    """
    Loop through participant-wise CSVs
    Append to output DF and save it to landing directory
    """

    os.chdir("89_Participant-Files")                                            # Target directory
    output = pd.DataFrame()                                                     # Empty DF to append into

    for file in tqdm(os.listdir()):
        # Append to output DF
        output = output.append(pd.read_csv(file), ignore_index=True, sort=False)

    output.fillna("NA", inplace=True)                                           # Fill NA values with 'NA'
    os.chdir("../99_Composite-CSV")                                             # Move to landing directory

    output.to_csv("{}.csv".format(filename), index=False, encoding="utf-8-sig")          
    os.chdir("../..")


def parse_responses(DATA, IX, LOG):
    """
    Wraps all of the above into a neat little function
    Warning: May work TOO well (just kidding)

    DATA <- JSON file of participant EMA reponses
    IX <- Participant's user name (stripped from list of JSON keys)
    LOG <- Text file to keep track of errors
    """

    filename = "{}.csv".format(IX)                                              # Format filename to be exported
    output_path = "89_Participant-Files/"                                       # Landing directory for output CSVs
    big_kahuna = pd.DataFrame()                                                 # Empty DF to append into

    for index in range(len(DATA.keys())):

        # Isolate pings, answers, and devices sub-branches from participant JSON branches

        # ------- Pings
        pings = pd.DataFrame(DATA[list(DATA.keys())[index]]["pings"])
        pings = pings.drop_duplicates(subset="startTime", keep="first").reset_index(drop=True)
        add_username(pings, IX)

        # ------- Answers
        answers = pd.DataFrame(DATA[list(DATA.keys())[index]]['answers'])
        clean = clean_answers(answers)
        PNA(clean)
        clean['data'] = clean['data'].apply(lambda x: x.strip("'"))

        flat_answers = flatten_answers(clean)

        # ------- Devices
        devices = pd.DataFrame(DATA[list(DATA.keys())[index]]['user']['installation']['device'], index=[0])
        app = pd.DataFrame(DATA[list(DATA.keys())[index]]['user']['installation']['app'], index=[0])
        user_total = devices.join(app)
        device_columns = user_total.columns

        # ------ Kick-out
        output_combo = pings.join(flat_answers, on="id")
        output_combo = output_combo.join(user_total, how="left")

        for var in device_columns:
            output_combo[var] = user_total[var][0]      # Fill length of DF columns with first device value

        big_kahuna = big_kahuna.append(output_combo, ignore_index=True, sort=False)

    try:
        split_nominations(big_kahuna)
    except Exception as e:
        LOG.write("Known {} for splitting nominations @ {}\n".format(type(e), IX))
    try:
        split_NSU(big_kahuna)
    except Exception as e:
        LOG.write("Cannot split NSU values for {}...{}\n".format(IX, type(e)))
    try:
        split_race(big_kahuna)
    except Exception as e:
        LOG.write("No race values provided for {}...{}\n".format(IX, type(e)))
        
    cleanup(big_kahuna)
    big_kahuna.to_csv(os.path.join(output_path, filename), index=False, encoding="utf-8-sig")

