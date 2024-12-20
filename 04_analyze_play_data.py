# %%
import pandas as pd
from datetime import datetime
import logging
import importlib

logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.WARNING,
    datefmt="%Y-%m-%d %I:%M %p",
)

# %% custom scripts for my project. Use import_module() since it starts with a number
make_play_data = importlib.import_module(name = '02_make_play_data')

# %%
season_type = [
    "Regular",
    "Post",
]  # can be Regular or Post. This is an input to determining scope of pbp and player dfs
season_year: int = 2024
data_path: str = "./Data/"

# TODO needs to be updated after playoff teams are announced
playoff_teams = [
    "BAL",
    "BUF",
    "KC",
    "HOU",
    "CLE",
    "MIA",
    "PIT",
    "SF",
    "DAL",
    "DET",
    "TB",
    "PHI",
    "LA",
    "GB",
]


# %%
teams = make_play_data.teams(season_year=season_year)

# %% list of team names
season_teams = teams["team_abbr"].to_list()

# %% update this to refresh data
roster_file = "roster_2024, 2024-12-20 021221 EST.parquet"
roster = make_play_data.rosters(file=roster_file, teams=teams)

# %%
k_file = "player_stats_kicking_2024, 2024-12-20 042746 EST.parquet"
k = make_play_data.kicker_stats(
    file=k_file, season_type=season_type, roster=roster, teams=teams
)

# %%
o_file = "player_stats_2024, 2024-12-20 042746 EST.parquet"
o = make_play_data.offense_stats(
    file=o_file, season_type=season_type, roster=roster, teams=teams
)

# %%
d_file = "play_by_play_2024, 2024-12-20 042156 EST.parquet"
d = make_play_data.defense_stats(
    file=d_file, season_type=season_type, teams=teams
)  # this function is not complete

# %%
pbp_file = "play_by_play_2024, 2024-12-20 042156 EST.parquet"
pbp = make_play_data.play_by_plays(file=pbp_file, season_type=season_type)
o_bonus = make_play_data.offense_bonus(
    file=pbp_file, season_type=season_type, roster=roster, teams=teams
)
d_bonus = make_play_data.defense_bonus(
    file=pbp_file, season_type=season_type, teams=teams
)  # this function is partially complete

# %%
# consolidate nfl stat dataframe
df_stats = pd.concat([o, o_bonus, k, d, d_bonus], ignore_index=True)

# clean up environment
del [[o, o_bonus, k, d, d_bonus]]

# %% update file name
fantasy_roster_file = "Consolidated Rosters, Gen 2024-12-20 1322.csv"
fantasy_rosters = pd.read_csv(data_path + fantasy_roster_file, engine="pyarrow")

# dict to check and replace column names
col_names = {
    "Fantasy Owner": "fantasy_owner",
    "Fantasy Team Name": "fantasy_team",
    "Automation Mapping": "player_id",  # technically doesn't apply for defense row
    "Roster": "roster_index",
    "Position Type": "position_type",
    "Position Code": "position_code",
    "Team Abbr.": "team_abbr",
}


def fix_name(df):
    return df.where()


# check if column order matches and then reassign column names
if (fantasy_rosters.columns == list(col_names.keys())).all():
    fantasy_rosters = fantasy_rosters.set_axis(list(col_names.values()), axis=1)

# %%
# split dataframe and merge depending on if player or defense
o = (
    fantasy_rosters.query("position_code != 'D'")
    .drop("team_abbr", axis=1)  # this will come from df_stats
    .merge(
        df_stats.query("player_id != 'N/A'"),  # exclude defense rows
        how="left",
        on="player_id",
    )
    .loc[
        lambda x: x.stat_label.notnull(), :
    ]  # exclude rows where there was no data to join
)

d = (
    fantasy_rosters.query("position_code == 'D'")
    .drop("player_id", axis=1)  # this will come from df_stats
    .merge(
        df_stats.query("player_id == 'N/A'"),  # include only defense rows
        how="left",
        on="team_abbr",
    )
)

# %%
df_fantasy_points = pd.concat(
    [o.astype({"subsequently_traded": bool}), d.astype({"subsequently_traded": bool})],
    ignore_index=True,
)
df_fantasy_points = df_fantasy_points[
    [
        "roster_index",
        "position_code",
        "fantasy_owner",
        "fantasy_team",
        "season_type",
        "week",
        "team_abbr",
        "team_conf",
        "team_division",
        "position_type",
        "position",
        "player_id",
        "player_name",
        "lookup_string",
        "subsequently_traded",
        "stat_label",
        "football_value",
        "fantasy_points",
    ]
]
df_fantasy_points.sort_values(["fantasy_team", "fantasy_owner", "roster_index"])

del [o, d]

# %%
output_file_name = f"Scored NFL Fantasy Rosters for {season_year}-{season_year+1} as of {datetime.today().strftime('%Y-%m-%d %H%M')}.parquet"

# %%
df_fantasy_points.to_parquet(data_path + output_file_name, engine="pyarrow")

# %%
