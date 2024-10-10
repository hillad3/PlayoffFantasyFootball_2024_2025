# %%
import pandas as pd
from numpy import floor

# %%
def make(file : str,
        season_type, 
        data_path = './Data/') -> pd.DataFrame:
  df = pd.read_parquet(data_path + file, engine='pyarrow')
  df.loc[df['season_type']=='REG','season_type'] = 'Regular'
  df.loc[df['season_type']=='POST','season_type'] = 'Post'
  df = df[df['season_type'].isin(season_type)]
  df.rename(columns = {'recent_team' : 'team_abbr'}, inplace=True)
  df = df[df['position'].isin(['QB', 'RB', 'FB', 'WR', 'TE'])]
  df.loc[df['position']=="FB",'position'] = 'RB'
  
  std_cols = [
    'position',
    'week',
    'season_type',
    'player_id',
    'player_name',
    'team_abbr'   
  ]

  stat_cols = [
    'passing_yards',
    'passing_tds',
    'rushing_yards',
    'rushing_tds',
    'receiving_yards',
    'receiving_tds',
    'interceptions',
    'sack_fumbles_lost',
    'rushing_fumbles_lost',
    'receiving_fumbles_lost',
    'passing_2pt_conversions',
    'rushing_2pt_conversions',
    'receiving_2pt_conversions'
  ]

  df = df.melt(
    id_vars = std_cols,
    value_vars = stat_cols,
    var_name = 'stat_label',
    value_name = 'football_value'
  )

  df.loc[df['football_value'].isna(),'football_value'] = 0 # clean up NaNs before calcs

  df.loc[(df['stat_label']=='passing_yards') & (df['football_value']>=400),'fantasy_points'] = floor(df['football_value']/50) + 2 # w bonus
  df.loc[(df['stat_label']=='passing_yards') & (df['football_value']<400),'fantasy_points'] = floor(df['football_value']/50)
  df.loc[(df['stat_label']=='rushing_yards') & (df['football_value']>=200),'fantasy_points'] = floor(df['football_value']/10) + 2 # w bonus
  df.loc[(df['stat_label']=='rushing_yards') & (df['football_value']<200),'fantasy_points'] = floor(df['football_value']/10)
  df.loc[(df['stat_label']=='receiving_yards') & (df['football_value']>=200),'fantasy_points'] = floor(df['football_value']/10) + 2 # w bonus
  df.loc[(df['stat_label']=='receiving_yards') & (df['football_value']<200),'fantasy_points'] = floor(df['football_value']/10)
  df.loc[df['stat_label'].isin(['passing_tds','rushing_tds','receiving_tds']),'fantasy_points'] = df['football_value'] * 6
  df.loc[df['stat_label'].isin(['passing_2pt_conversions','rushing_2pt_conversions','receiving_2pt_conversions']),'fantasy_points'] = df['football_value'] * 2
  df.loc[df['stat_label'].isin(['interceptions','fantasy_points','fantasy_points']),'fantasy_points'] = df['football_value'] * -2
  df.loc[df['stat_label'].isin(['sack_fumbles_lost','rushing_fumbles_lost','receiving_fumbles_lost']),'fantasy_points'] = df['football_value'] * -2

  df['fantasy_points'] = df['fantasy_points'].astype('Int64') # fantasy points are always integers

  df.loc[df['fantasy_points'].isna(),'fantasy_points'] = 0 # fill in any NaNs if a rule did not apply

  return df