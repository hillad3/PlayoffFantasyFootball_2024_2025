# %% 
import pandas as pd
import numpy as np
import nfl_data_py as nfl
import logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %I:%M %p')

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
teams = make_play_data.teams(season_year)
teams.head()

# %% list of team names
season_teams = teams['team_abbr'].to_list()
print(season_teams)

# %%
roster_file = 'roster_2024, 2024-10-28 031411 EDT.parquet'
roster = make_play_data.rosters(roster_file, teams = teams)
roster.head()

# %%
pbp_file = 'play_by_play_2024, 2024-10-28 052227 EDT.parquet'
o_bonus = make_play_data.offense_bonus(file = pbp_file, season_type = season_type)
o_bonus.info()

# %%
k_file = 'player_stats_kicking_2024, 2024-10-28 052851 EDT.parquet'
k = make_play_data.kickers(file=k_file, season_type=season_type)
k.info()

# %% 
o_file = 'player_stats_2024, 2024-10-28 052851 EDT.parquet'
o = make_play_data.offense_stats(file=o_file, season_type=season_type)
o.info()

# %%
d_file ='player_stats_def_2024, 2024-10-28 052851 EDT.parquet'
d = make_play_data.defense_stats(file=d_file, season_type=season_type)
d.info()

# %%
