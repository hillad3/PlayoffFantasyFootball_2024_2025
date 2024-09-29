

# %%
import pandas as pd

pbp = pd.read_parquet('Data/play_by_play_2024.parquet', engine='pyarrow')

season_int = 2023
season_type = ['REG','POST']
season_teams = [
  'ARI','ATL','BAL','BUF','CAR',
  'CHI','CIN','CLE','DAL','DEN',
  'DET','GB','HOU','IND','JAX',
  'KC','LA','LAC','LV','MIA',
  'MIN','NE','NO','NYG','NYJ',
  'PHI','PIT','SEA','SF','TB',
  'TEN','WAS']

# TODO needs to be updated after playoff teams are announced
playoff_teams = [
  'BAL','BUF','KC','HOU','CLE',
  'MIA','PIT','SF','DAL','DET',
  'TB','PHI','LA','GB'
]