# %% 
import pandas as pd
import numpy as np
import nfl_data_py as nfl
import logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %I:%M %p')

# %% these are custom scripts for my project
import team_info
import roster_lineup
import play_by_play_stats
import kicker_stats
import offense_stats

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
teams = team_info.get(season_year)
teams.head()

# %% list of team names
season_teams = teams['team_abbr'].to_list()
print(season_teams)

# %%
roster_file = 'roster_2024, 2024-10-07 031250 EDT.parquet'
roster = roster_lineup.get(roster_file, teams = teams)
roster.head()

# %%
pbp_file = 'play_by_play_2024, 2024-10-07 052118 EDT.parquet'
pbp = play_by_play_stats.make(file = pbp_file, season_type = season_type)
pbp.info()

# %%
bonus_stats = play_by_play_stats.offense_bonuses(pbp)
bonus_stats.info(10)

# %%
k_file = 'player_stats_kicking_2024, 2024-10-07 052613 EDT.parquet'
k = kicker_stats.make(file=k_file, season_type=season_type)
k.info()

# %% 
o_file = 'player_stats_2024, 2024-10-07 052613 EDT.parquet'
o = offense_stats.make(file=o_file, season_type=season_type)
o.info()

# %%
d_file ='player_stats_def_2024, 2024-10-07 052613 EDT.parquet'
d = pd.read_parquet(data_path + d_file, engine='pyarrow')
d.info()

# %%
