# %%
import pandas as pd
import numpy as np
import os
from datetime import datetime

# %%
# update with roster directory
roster_folder = "Data/Individual Rosters/"
files = os.listdir(roster_folder)

# %%
for file in files:
    if file.endswith(".xlsx") and "Playoff Fantasy Roster 2024" in file:
        print(
            "WARNING: There are *.xlsx files still remaining in the directory: " + file
        )


# %%
# expected data for assertions
expected_columns = [
    "Fantasy Owner",
    "Fantasy Owner Email",
    "Fantasy Team Name",
    "Automation Mapping",
    "Roster",
    "Position Type",
    "Position Code",
    "Position Group",
    "Team Abbr.",
    "Selection",
    "Check 1 - Selection is Unique",
    "Check 2 - Team is Unique",
]
minimum_positions = [
    "K",
    "QB1",
    "QB2",
    "QB3",
    "RB1",
    "RB2",
    "RB3",
    "TE1",
    "TE2",
    "WR1",
    "WR2",
    "WR3",
    "D",
]
extra_positions = ["RB4", "WR4", "TE3"]

# %%
# empty dataframe
df = pd.DataFrame()

# empty list to check team names
team_names = []

# cycle through each roster and perform checks, then concat
for file in files:
    if file.endswith(".csv") and "Playoff Fantasy Roster 2024" in file:
        print("Checking: " + file)
        tmp_df = pd.read_csv(roster_folder + file)
        tmp_positions = tmp_df["Position Code"].to_list()
        tmp_teamname = tmp_df["Fantasy Team Name"][1]

        assert (
            len(tmp_df.columns) == 12
        )  # check that the roster has correct number columns
        assert all(
            x in expected_columns for x in tmp_df.columns.values
        )  # check that the column names match
        assert len(tmp_df) == 14  # check that each roster is correct length
        assert tmp_df[
            "Automation Mapping"
        ].is_unique  # check all automation mapping is unique
        assert not tmp_df[
            "Fantasy Team Name"
        ].is_unique  # check all team names are the same
        assert (
            tmp_teamname not in team_names
        )  # check that no other team has the same team name
        if tmp_teamname not in team_names:
            team_names.append(tmp_df["Fantasy Team Name"][1])
        assert set(minimum_positions).issubset(
            tmp_positions
        )  # check minimum positions are present

        # determine which position is the extra position for the tmp roster
        tmp_counts = []
        for p in tmp_positions:
            if p in minimum_positions:
                None
            else:
                tmp_counts.append(p)
        assert set(tmp_counts).issubset(
            extra_positions
        )  # check that there is an extra position
        assert (
            len(tmp_counts) == 1
        )  # check that there is only one extra position (should pass if previous checks passed)
        assert (
            not df.isnull().values.any()
        )  # check that there are no NaNs in the dataset

        # concat csv files into one df
        df = pd.concat([df, pd.read_csv(roster_folder + file)])

# truncate the name
df["Fantasy Owner"] = df["Fantasy Owner"].str.extract("(^.* .)")

# drop unnecessary columns and columns with personal information
df.drop(
    columns=[
        "Fantasy Owner Email",
        "Position Group",
        "Selection",
        "Check 1 - Selection is Unique",
        "Check 2 - Team is Unique",
    ],
    inplace=True,
)
df.head()

# %%
output_file = (
    "Consolidated Rosters, Gen "
    + str(datetime.now().strftime("%Y-%m-%d %H%M"))
    + ".csv"
)
df.to_csv("Data/" + output_file, index=False)
