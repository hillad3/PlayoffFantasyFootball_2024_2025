# %%
import pandas as pd

# %%
def get(file : str,
        season_type, 
        data_path = './Data/') -> pd.DataFrame:
  df = pd.read_parquet(data_path + file, engine='pyarrow')
  df.loc[df['season_type']=='REG','season_type'] = 'Regular'
  df.loc[df['season_type']=='POST','season_type'] = 'Post'
  df = df[df['season_type'].isin(season_type)]
  df.rename(columns = {'team' : 'team_abbr'}, inplace=True)
  df['position'] = 'K' # position is not in the original dataset
  df['fg_made_50plus'] = df['fg_made_50_59'] + df['fg_made_60_']

  std_cols = [
    'position',
    'week',
    'season_type',
    'player_id',
    'player_name',
    'team_abbr'   
  ]

  stat_cols = [
    'fg_made',
    'fg_made_40_49',
    'fg_made_50plus',
    'fg_missed',
    'fg_blocked', # this doesn't have a rule for it currently
    'pat_made',
    'pat_missed'
  ]

  df = df.melt(
    id_vars = std_cols,
    value_vars = stat_cols,
    var_name = 'stat_label',
    value_name = 'football_value'
  )

  df.loc[df['football_value'].isna(),'football_value'] = 0 # clean up NaNs before calcs

  df.loc[df['stat_label']=='fg_made','fantasy_points'] = df['football_value'] * 3
  df.loc[df['stat_label']=='fg_made_40_49','fantasy_points'] = df['football_value'] * 1
  df.loc[df['stat_label']=='fg_made_50plus','fantasy_points'] = df['football_value'] * 2
  df.loc[df['stat_label']=='fg_made_missed','fantasy_points'] = df['football_value'] * -1
  df.loc[df['stat_label']=='pat_made','fantasy_points'] = df['football_value'] * 1
  df.loc[df['stat_label']=='pat_missed','fantasy_points'] = df['football_value'] * -1
  
  df.loc[df['fantasy_points'].isna(),'fantasy_points'] = 0 # fill in any NaNs if a rule did not apply

  return df
