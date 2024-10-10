# %%
import pandas as pd
import numpy as np
import logging

# %%
def get(roster_file,
        teams : pd.DataFrame, 
        path : str = './Data/') -> pd.DataFrame:
  roster = pd.read_parquet(path + roster_file, engine='pyarrow')
  logging.info('Initial roster length: ' + str(len(roster)))
  roster.rename(columns={'full_name':'player_name', 'gsis_id':'player_id','team':'team_abbr'}, inplace=True)
  roster = roster[['season','team_abbr','position','status','player_name', 'player_id']]
  roster = roster.merge(teams.drop(['team_name','team_logo_espn','position', 'lookup_string'], axis=1), how='left', on='team_abbr')
  logging.info('Roster length after merge: ' + str(len(roster)))
  logging.info('Full list of positions: ' + ', '.join(roster['position'].unique()))
  roster = roster[roster['position'].isin(['QB', 'P', 'K', 'TE', 'RB', 'WR'])]
  logging.info('Roster length after filtering for offensive team positions: ' + str(len(roster)))
  roster['position'] = np.where(roster['position']=="P","K",roster['position'])
  logging.info('Remaining list of positions: ' + ', '.join(roster['position'].unique()))
  roster['lookup_string'] = roster['position'] + ", " + roster['team_abbr'] + ": " + roster['player_name'] + " (" + roster['team_division'] + ")"
  return roster
