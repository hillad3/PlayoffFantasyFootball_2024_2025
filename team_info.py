# %% 
import pandas as pd
import nfl_data_py as nfl

def get(season : int, 
        teams_file : str = 'nflreadr_teams.csv', 
        path : str = './Data/') -> pd.DataFrame:
  teams_file = 'nflreadr_teams.csv'
  teams = pd.read_csv(path + teams_file, engine='pyarrow')
  teams = teams[teams['season'] == season]
  teams['team_name_w_abbr'] = teams['full'] + " (" + teams['team'] + ")"
  teams = teams[['team','full','team_name_w_abbr']]
  teams.rename(columns={'team':'team_abbr', 'full':'team_name'}, inplace=True)
  teams = teams.merge(nfl.import_team_desc()[['team_abbr','team_conf','team_division','team_logo_espn']], how='left', on='team_abbr') # add in conf & division
  teams['position'] = "Defense" # when selecting a team in the fantasy rules, it is always as a defense
  teams['lookup_string'] = teams['position'] + ", " + teams['team_abbr'] + " (" + teams['team_division'] + ")"
  teams = teams[['team_name', 'team_abbr', 'team_name_w_abbr', 'team_conf', 'team_division', 'position', 'lookup_string', 'team_logo_espn']]
  return teams
