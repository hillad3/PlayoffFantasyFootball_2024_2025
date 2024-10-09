# %% 
import pandas as pd
import numpy as np
import nfl_data_py as nfl
import logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %I:%M %p')

# %% these are custom scripts for my project
import create_teams_df
import create_roster_df
import create_pbp_df
import create_k_player_df

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
teams = create_teams_df.get(season_year)
teams.head()

# %% list of team names
season_teams = teams['team_abbr'].to_list()
print(season_teams)

# %%
roster_file = 'roster_2024, 2024-10-07 031250 EDT.parquet'
roster = create_roster_df.get(roster_file, teams = teams)
roster.head()

# %%
pbp_file = 'play_by_play_2024, 2024-10-07 052118 EDT.parquet'
pbp = create_pbp_df.get(file = pbp_file, season_type = season_type)
pbp.head()

# %%
k_file = 'player_stats_kicking_2024, 2024-10-07 052613 EDT.parquet'
k = create_k_player_df.get(file=k_file,season_type=season_type)
k.head()

# %% 
o_file = 'player_stats_2024, 2024-10-07 052613 EDT.parquet'
o = pd.read_parquet(data_path + o_file, engine='pyarrow')
o.info()

# %%
d_file ='player_stats_def_2024, 2024-10-07 052613 EDT.parquet'
d = pd.read_parquet(data_path + d_file, engine='pyarrow')
d.info()

# %%
