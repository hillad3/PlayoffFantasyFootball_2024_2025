# %% 
import pandas as pd
import numpy as np
import nfl_data_py as nfl
import logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.WARNING, datefmt='%Y-%m-%d %I:%M %p')

# %% custom scripts for my project
import make_play_data

# %%
season_type = ['Regular','Post'] # can be Regular or Post. This is an input to determining scope of pbp and player dfs
season_year : int = 2024
data_path : str = './Data/'

# TODO needs to be updated after playoff teams are announced
playoff_teams = [
  'BAL','BUF','KC','HOU','CLE',
  'MIA','PIT','SF','DAL','DET',
  'TB','PHI','LA','GB'
]

# %%
teams = make_play_data.teams(season_year = season_year)

# %% list of team names
season_teams = teams['team_abbr'].to_list()

# %%
roster_file = 'roster_2024, 2024-10-28 031411 EDT.parquet'
roster = make_play_data.rosters(file = roster_file, teams = teams)

# %%
k_file = 'player_stats_kicking_2024, 2024-10-28 052851 EDT.parquet'
k = make_play_data.kicker_stats(file = k_file, season_type = season_type, roster = roster, teams = teams)

# %% 
o_file = 'player_stats_2024, 2024-10-28 052851 EDT.parquet'
o = make_play_data.offense_stats(file = o_file, season_type = season_type, roster = roster, teams = teams)

# %%
d_file ='play_by_play_2024, 2024-10-28 052227 EDT.parquet'
d = make_play_data.defense_stats(file = d_file, season_type = season_type, teams = teams) # this function is not complete

# %%
pbp_file = 'play_by_play_2024, 2024-10-28 052227 EDT.parquet'
pbp = make_play_data.play_by_plays(file = pbp_file, season_type = season_type)
o_bonus = make_play_data.offense_bonus(file = pbp_file, season_type = season_type, roster = roster, teams = teams)
d_bonus = make_play_data.defense_bonus(file = pbp_file, season_type = season_type, teams = teams) # this function is partially complete

# %%
# consolidate nfl stat dataframe
df_stats = pd.concat([
  o,
  o_bonus,
  k,
  d,
  d_bonus
])

# clean up environment
del [[
  o,
  o_bonus,
  k,
  d,
  d_bonus
]]

# %%
fantasy_roster_file = "Consolidated Rosters, Gen 2024-09-29 0926.csv"
fantasy_rosters = pd.read_csv(data_path + fantasy_roster_file, engine="pyarrow")

# dict to check and replace column names
col_names = {
    'Fantasy Owner':'fantasy_owner',
    'Fantasy Team Name':'fantasy_team',
    'Automation Mapping':'player_id', # technically doesn't apply for defense row
    'Roster':'roster_index',
    'Position Type':'position_type',
    'Position Code':'position_code',
    'Team Abbr.':'team_abbr'}

def fix_name(df):
  return df.where()

# check if column order matches and then reassign column names
if (fantasy_rosters.columns == list(col_names.keys())).all():
  fantasy_rosters = (
    fantasy_rosters
    .set_axis(list(col_names.values()), axis = 1)
  )

# %%
# split dataframe and merge depending on if player or defense
o = (
  fantasy_rosters
  .query("position_code != 'D'")
  .merge(
    df_stats.query("player_id != 'N/A'"), # exclude defense rows
    how = 'outer',
    on = 'player_id'
  )
)

d = (
  fantasy_rosters
  .query("position_code == 'D'")
  .merge(
    df_stats.query("player_id == 'N/A'"), # include only defense rows
    how = 'outer',
    on = 'team_abbr'
  )
)


# TODO need to figure out why there are N/As. I think because of players changing teams so need to consider how to do joins and which team_abbr to use (even if that doesn't align with the lookup_string)


# output_file <- paste0(
#   "Data/Output/Scored Rosters/NFL Fantasy Scores for 2023-2024 as of ",
#   str_remove_all(Sys.time(), ":"),
#   ".csv"
# )

# fwrite(scored_rosters, output_file)
