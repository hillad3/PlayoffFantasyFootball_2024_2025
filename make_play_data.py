# %%
import pandas as pd
import nfl_data_py as nfl
import numpy as np
from functools import reduce
import logging

# %%
def rosters(roster_file,
        teams : pd.DataFrame, 
        path : str = './Data/') -> pd.DataFrame:
  roster = pd.read_parquet(path + roster_file, engine='pyarrow')
  logging.info('Initial roster length: ' + str(len(roster)))
  roster.rename(columns={'full_name':'player_name', 'gsis_id':'player_id','team':'team_abbr'}, inplace=True)
  roster = roster[['season','team_abbr','position','status','player_name', 'player_id']]
  roster = roster.merge(teams.drop(['team_name','team_logo_espn','position', 'lookup_string'], axis=1), how='left', on='team_abbr')
  logging.info('Roster length after merge: ' + str(len(roster)))
  logging.info('Full list of positions: ' + ', '.join(roster['position'].unique()))
  roster = roster[roster['position'].isin(['QB', 'P', 'K', 'TE', 'RB', 'WR'])] # it doesn't look like there is a FB position for 2024
  logging.info('Roster length after filtering for offensive team positions: ' + str(len(roster)))
  roster['position'] = np.where(roster['position']=="P","K",roster['position'])
  logging.info('Remaining list of positions: ' + ', '.join(roster['position'].unique()))
  roster['lookup_string'] = roster['position'] + ", " + roster['team_abbr'] + ": " + roster['player_name'] + " (" + roster['team_division'] + ")"
  return roster

def teams(season : int, 
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


# %%
def offense_stats(file : str,
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

  df.loc[(df['stat_label']=='passing_yards') & (df['football_value']>=400),'fantasy_points'] = np.floor(df['football_value']/50) + 2 # w bonus
  df.loc[(df['stat_label']=='passing_yards') & (df['football_value']<400),'fantasy_points'] = np.floor(df['football_value']/50)
  df.loc[(df['stat_label']=='rushing_yards') & (df['football_value']>=200),'fantasy_points'] = np.floor(df['football_value']/10) + 2 # w bonus
  df.loc[(df['stat_label']=='rushing_yards') & (df['football_value']<200),'fantasy_points'] = np.floor(df['football_value']/10)
  df.loc[(df['stat_label']=='receiving_yards') & (df['football_value']>=200),'fantasy_points'] = np.floor(df['football_value']/10) + 2 # w bonus
  df.loc[(df['stat_label']=='receiving_yards') & (df['football_value']<200),'fantasy_points'] = np.floor(df['football_value']/10)
  df.loc[df['stat_label'].isin(['passing_tds','rushing_tds','receiving_tds']),'fantasy_points'] = df['football_value'] * 6
  df.loc[df['stat_label'].isin(['passing_2pt_conversions','rushing_2pt_conversions','receiving_2pt_conversions']),'fantasy_points'] = df['football_value'] * 2
  df.loc[df['stat_label'].isin(['interceptions','fantasy_points','fantasy_points']),'fantasy_points'] = df['football_value'] * -2
  df.loc[df['stat_label'].isin(['sack_fumbles_lost','rushing_fumbles_lost','receiving_fumbles_lost']),'fantasy_points'] = df['football_value'] * -2

  df['fantasy_points'] = df['fantasy_points'].astype('Int64') # fantasy points are always integers

  df.loc[df['fantasy_points'].isna(),'fantasy_points'] = 0 # fill in any NaNs if a rule did not apply

  return df

# %%
def kicker_stats(file : str, season_type, data_path = './Data/') -> pd.DataFrame:
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

  df['fantasy_points'] = df['fantasy_points'].astype('Int64') # fantasy points are always integers
  
  df.loc[df['fantasy_points'].isna(),'fantasy_points'] = 0 # fill in any NaNs if a rule did not apply

  return df

# %%
def defense_stats(file : str,
        season_type, 
        data_path = './Data/') -> pd.DataFrame:
  df = pd.read_parquet(data_path + file, engine='pyarrow')
  df.loc[df['season_type']=='REG','season_type'] = 'Regular'
  df.loc[df['season_type']=='POST','season_type'] = 'Post'
  df = df[df['season_type'].isin(season_type)]
  df.rename(columns = {'team' : 'team_abbr'}, inplace=True)

  std_cols = [
    'week',
    'season_type',
    'player_id',
    'player_name',
    'team_abbr'   
  ]

  stat_cols = [
    'def_sacks',
    'def_interceptions',
    'def_fumbles',
    'def_safety'
  ]

  df = df.melt(
    id_vars = std_cols,
    value_vars = stat_cols,
    var_name = 'stat_label',
    value_name = 'football_value'
  )

  return df

# %%
def play_by_plays(file : str, 
        season_type,
        path : str = './Data/') -> pd.DataFrame:
  pbp = pd.read_parquet(path = path + file, engine='pyarrow')
  pbp.loc[(pbp['season_type']=="REG"),'season_type'] = "Regular"
  pbp.loc[(pbp['season_type']=="POST"),'season_type'] = "Post"
  pbp = pbp[pbp['season_type'].isin(season_type)]
  pbp = pbp[[
    'game_id',
    'game_date',
    'week',
    'season_type',
    'home_team',
    'away_team',
    'home_score',
    'away_score',
    'posteam',
    'defteam',
    'play_type',
    'time',
    'desc',
    'fixed_drive_result',
    'touchdown',
    'pass_touchdown',
    'rush_touchdown',
    'return_touchdown',
    'yards_gained',
    'rushing_yards',
    'passing_yards',
    'return_yards',
    'return_team',
    'interception',
    'interception_player_name',
    'interception_player_id',
    'fumble',
    'fumble_lost',
    'fumble_recovery_1_team',
    'passer_player_name',
    'passer_player_id',
    'receiver_player_name',
    'receiver_player_id',
    'rusher_player_name',
    'rusher_player_id',
    'td_player_name',
    'td_player_id',
    'kicker_player_name',
    'kicker_player_id',
    'kickoff_returner_player_name',
    'kickoff_returner_player_id',
    'punt_returner_player_name',
    'punt_returner_player_id',
    'fumbled_1_player_name',
    'fumbled_1_player_id',
    'fumble_recovery_1_player_name',
    'fumble_recovery_1_player_id',
    'sack',
    'sack_player_name',
    'sack_player_id',
    'half_sack_1_player_name',
    'half_sack_1_player_id',
    'half_sack_2_player_name',
    'half_sack_2_player_id',
    'safety',
    'safety_player_name',
    'safety_player_id',
    'two_point_conv_result',
    'two_point_attempt',
    'extra_point_result',
    'extra_point_attempt',
    'field_goal_result',
    'field_goal_attempt',
    'kick_distance',
    'blocked_player_name',
    'blocked_player_id'
  ]]
  return pbp

pbp = play_by_plays('play_by_play_2024, 2024-10-28 052227 EDT.parquet', ['Regular','Post'])

# %% 
def offense_bonus(file : str, 
        season_type,
        roster : pd.DataFrame,
        path : str = './Data/') -> pd.DataFrame:

  # create the play-by-play data that will be used to determine the bonuses      
  pbp = play_by_plays(file, season_type, path)

  # offensive bonus for touchdown with pass over 40 yards for qb
  forty_yd_plus_passing_td_qb_bonus = pbp[(pbp['pass_touchdown']==1) & (pbp['passing_yards']>=40)].copy()
  forty_yd_plus_passing_td_qb_bonus = forty_yd_plus_passing_td_qb_bonus[['week','season_type','posteam','passer_player_id','passer_player_name','pass_touchdown','passing_yards']]
  forty_yd_plus_passing_td_qb_bonus.rename(columns={'posteam':'team_abbr','passer_player_name':'player_name','passer_player_id':'player_id'}, inplace=True)
  forty_yd_plus_passing_td_qb_bonus['stat_label'] = "forty_yd_plus_passing_td_qb_bonus"
  forty_yd_plus_passing_td_qb_bonus = forty_yd_plus_passing_td_qb_bonus.groupby(['week', 'season_type', 'team_abbr', 'player_name', 'player_id','stat_label'], as_index=False).size()
  forty_yd_plus_passing_td_qb_bonus.rename(columns={'size':'football_value'},inplace=True)
  forty_yd_plus_passing_td_qb_bonus['fantasy_value'] = forty_yd_plus_passing_td_qb_bonus['football_value']*2

  # offensive bonus for touchdown with pass over 40 yards for receiver
  forty_yd_plus_passing_td_receiver_bonus = pbp[(pbp['pass_touchdown']==1) & (pbp['passing_yards']>=40)].copy()
  forty_yd_plus_passing_td_receiver_bonus = forty_yd_plus_passing_td_receiver_bonus[['week','season_type','posteam','receiver_player_id','receiver_player_name','pass_touchdown','passing_yards']]
  forty_yd_plus_passing_td_receiver_bonus.rename(columns={'posteam':'team_abbr','receiver_player_name':'player_name','receiver_player_id':'player_id'}, inplace=True)
  forty_yd_plus_passing_td_receiver_bonus['stat_label'] = "forty_yd_plus_passing_td_receiver_bonus"
  forty_yd_plus_passing_td_receiver_bonus = forty_yd_plus_passing_td_receiver_bonus.groupby(['week', 'season_type', 'team_abbr', 'player_name', 'player_id','stat_label'], as_index=False).size()
  forty_yd_plus_passing_td_receiver_bonus.rename(columns={'size':'football_value'},inplace=True)
  forty_yd_plus_passing_td_receiver_bonus['fantasy_value'] = forty_yd_plus_passing_td_receiver_bonus['football_value']*2

  # offensive bonus for touchdown with rush over 40 yards for rusher
  forty_yd_plus_rushing_td_bonus = pbp[(pbp['rush_touchdown']==1) & (pbp['rushing_yards']>=40)].copy()
  forty_yd_plus_rushing_td_bonus = forty_yd_plus_rushing_td_bonus[['week','season_type','posteam','rusher_player_id','rusher_player_name','rush_touchdown','rushing_yards']]
  forty_yd_plus_rushing_td_bonus.rename(columns={'posteam':'team_abbr','rusher_player_name':'player_name','rusher_player_id':'player_id'}, inplace=True)
  forty_yd_plus_rushing_td_bonus['stat_label'] = "forty_yd_plus_rushing_td_bonus"
  forty_yd_plus_rushing_td_bonus = forty_yd_plus_rushing_td_bonus.groupby(['week', 'season_type', 'team_abbr', 'player_name', 'player_id','stat_label'], as_index=False).size()
  forty_yd_plus_rushing_td_bonus.rename(columns={'size':'football_value'},inplace=True)
  forty_yd_plus_rushing_td_bonus['fantasy_value'] = forty_yd_plus_rushing_td_bonus['football_value']*2

  # offensive bonus for touchdown return over 40 yards for receiving team
  # only for normal possession plays by the opposite team (i.e. pass or rush) 
  # in a kickoff, the receiving team is listed as posteam
  # in a punt, the receiving team is listed as the defteam
  forty_yd_plus_kickoff_return_td_bonus = pbp[(pbp['play_type']=="kickoff") & (pbp['return_touchdown']==1) & (pbp['return_yards']>=40)].copy()
  forty_yd_plus_kickoff_return_td_bonus = forty_yd_plus_kickoff_return_td_bonus[['week','season_type','posteam','kickoff_returner_player_id','kickoff_returner_player_name','return_touchdown','return_yards']]
  forty_yd_plus_kickoff_return_td_bonus.rename(columns={'posteam':'team_abbr','kickoff_returner_player_name':'player_name','kickoff_returner_player_id':'player_id'}, inplace=True)
  forty_yd_plus_kickoff_return_td_bonus['stat_label'] = "forty_yd_plus_kickoff_return_td_bonus"
  forty_yd_plus_kickoff_return_td_bonus = forty_yd_plus_kickoff_return_td_bonus.groupby(['week', 'season_type', 'team_abbr', 'player_name', 'player_id','stat_label'], as_index=False).size()
  forty_yd_plus_kickoff_return_td_bonus.rename(columns={'size':'football_value'},inplace=True)
  forty_yd_plus_kickoff_return_td_bonus['fantasy_value'] = forty_yd_plus_kickoff_return_td_bonus['football_value']*2

  forty_yd_plus_punt_return_td_bonus = pbp[(pbp['play_type']=="punt") & (pbp['return_touchdown']==1) & (pbp['return_yards']>=40)].copy()
  forty_yd_plus_punt_return_td_bonus = forty_yd_plus_punt_return_td_bonus[['week','season_type','defteam','punt_returner_player_id','punt_returner_player_name','return_touchdown','return_yards']]
  forty_yd_plus_punt_return_td_bonus.rename(columns={'defteam':'team_abbr','punt_returner_player_name':'player_name','punt_returner_player_id':'player_id'}, inplace=True)
  forty_yd_plus_punt_return_td_bonus['stat_label'] = "forty_yd_plus_punt_return_td_bonus"
  forty_yd_plus_punt_return_td_bonus = forty_yd_plus_punt_return_td_bonus.groupby(['week', 'season_type', 'team_abbr', 'player_name', 'player_id','stat_label'], as_index=False).size()
  forty_yd_plus_punt_return_td_bonus.rename(columns={'size':'football_value'},inplace=True)
  forty_yd_plus_punt_return_td_bonus['fantasy_value'] = forty_yd_plus_punt_return_td_bonus['football_value']*2

  o_bonuses = pd.concat([forty_yd_plus_passing_td_qb_bonus, 
           forty_yd_plus_passing_td_receiver_bonus, 
           forty_yd_plus_rushing_td_bonus, 
           forty_yd_plus_kickoff_return_td_bonus, 
           forty_yd_plus_punt_return_td_bonus])

  o_bonuses = pd.merge(
    o_bonuses[['player_id','team_abbr','week','season_type','stat_label','football_value','fantasy_value']],
    roster[['player_id','team_abbr','team_division','team_conf','lookup_string','position','player_name']],
    how="left",
    on=['player_id','team_abbr'],
  )

  if o_bonuses['lookup_string'].notnull().all(): 
    logging.warning("There are stat rows that did not join with a row in the roster data frame.")

  o_bonuses = o_bonuses[['week','season_type','team_abbr','team_conf','team_division','position','player_id','player_name','lookup_string','stat_label','football_value','fantasy_value']]

  o_bonuses.loc[o_bonuses['position']=='FB','position'] = 'RB' # I don't think reassignment will be needed since FB isn't in the 2024 roster data currently, but it was needed in 2023

  o_bonuses_included = o_bonuses.loc[o_bonuses['position'].isin(['QB','RB','WR','TE'])]
  o_bonuses_excluded = o_bonuses.loc[np.logical_not(o_bonuses['position'].isin(['QB','RB','WR','TE']))]

  if len(o_bonuses_excluded) > 0:
    logging.warning("There are o_bonuses excluded because the position is out of scope:")
    logging.warning(o_bonuses_excluded)

  o_bonuses_included = o_bonuses_included.sort_values(by=['week','position'])
  
  return o_bonuses_included

# %%
def defense_bonus(file : str, 
        season_type,
        path : str = './Data/') -> pd.DataFrame:

  pbp = play_by_plays(file, season_type, path)

  # defensive bonus for sacks
  # when sack==1, either there is a single person sack and "sack_player_name" will not be null, or
  # there are two half_sack_players (but sack_player_name will be null). sack==1 includes instances when
  # then the qb gets back to the line of scrimmage (approximately) so it is possible to exclude those
  # instances by applyling a filter of yards_gained < 0L. Finally, there may be instances where the sack_player 
  # is not recorded but there was still a sack recorded (seemingly if a fumble happens in the same play).
  # Therefore it is best to only filter by sack==1.
  sack_bonus = pbp[(pbp['sack']==1)].copy()
  sack_bonus = sack_bonus[['week','season_type','defteam','sack_player_id','sack_player_name','half_sack_1_player_id','half_sack_1_player_name','half_sack_2_player_id','half_sack_2_player_name']]
  sack_bonus.rename(columns={'defteam':'team_abbr'}, inplace=True)
  sack_bonus['stat_label'] = "sack_bonus"
  sack_bonus = sack_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  sack_bonus.rename(columns={'size':'football_value'},inplace=True)
  sack_bonus['fantasy_value'] = sack_bonus['football_value']

  # defensive bonus for safeties
  safety_bonus = pbp[(pbp['safety']==1) & (pbp['safety_player_id'].notnull())].copy()
  safety_bonus = safety_bonus[['week','season_type','defteam','safety_player_id','safety_player_name']]
  safety_bonus.rename(columns={'defteam':'team_abbr'}, inplace=True)
  safety_bonus['stat_label'] = "safety_bonus"
  safety_bonus = safety_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  safety_bonus.rename(columns={'size':'football_value'},inplace=True)
  safety_bonus['fantasy_value'] = safety_bonus['football_value']

  # defensive bonus for fumble recovery
  fumble_recovery_bonus = pbp[(pbp['fumble']==1) & (pbp['fumble_lost']==1) & (pbp['play_type']!="punt")].copy()
  fumble_recovery_bonus = fumble_recovery_bonus[['week','season_type','defteam','fumbled_1_player_id','fumbled_1_player_name','fumble_recovery_1_player_id','fumble_recovery_1_player_name']]
  fumble_recovery_bonus.rename(columns={'defteam':'team_abbr'}, inplace=True)
  fumble_recovery_bonus['stat_label'] = "fumble_recovery_bonus"
  fumble_recovery_bonus = fumble_recovery_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  fumble_recovery_bonus.rename(columns={'size':'football_value'},inplace=True)
  fumble_recovery_bonus['fantasy_value'] = fumble_recovery_bonus['football_value']*2

  # defensive bonus for fumble recovery during a punt; use posteam as team_abbr
  fumble_recovery_punt_bonus = pbp[(pbp['fumble']==1) & (pbp['fumble_lost']==1) & (pbp['play_type']=="punt")].copy()
  fumble_recovery_punt_bonus = fumble_recovery_punt_bonus[['week','season_type','posteam','fumbled_1_player_id','fumbled_1_player_name','fumble_recovery_1_player_id','fumble_recovery_1_player_name']]
  fumble_recovery_punt_bonus.rename(columns={'posteam':'team_abbr'}, inplace=True)
  fumble_recovery_punt_bonus['stat_label'] = "fumble_recovery_punt_bonus"
  fumble_recovery_punt_bonus = fumble_recovery_punt_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  fumble_recovery_punt_bonus.rename(columns={'size':'football_value'},inplace=True)
  fumble_recovery_punt_bonus['fantasy_value'] = fumble_recovery_punt_bonus['football_value']*2

  # defensive bonus for interception
  interception_bonus = pbp[(pbp['interception']==1)].copy()
  interception_bonus = interception_bonus[['week','season_type','defteam','interception_player_id','interception_player_name']]
  interception_bonus.rename(columns={'defteam':'team_abbr'}, inplace=True)
  interception_bonus['stat_label'] = "interception_bonus"
  interception_bonus = interception_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  interception_bonus.rename(columns={'size':'football_value'},inplace=True)
  interception_bonus['fantasy_value'] = interception_bonus['football_value']*2

  # defensive bonus for block
  block_bonus = pbp[(pbp['blocked_player_name'].notnull())].copy()
  block_bonus = block_bonus[['week','season_type','defteam','blocked_player_id','blocked_player_name']]
  block_bonus.rename(columns={'defteam':'team_abbr'}, inplace=True)
  block_bonus['stat_label'] = "block_bonus"
  block_bonus = block_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  block_bonus.rename(columns={'size':'football_value'},inplace=True)
  block_bonus['fantasy_value'] = block_bonus['football_value']*2
  
  # defensive bonus for defensive td return
  def_td_return_bonus = pbp[(pbp['return_touchdown']==1) & (pbp['play_type'].isin(['pass','run']))].copy()
  def_td_return_bonus = def_td_return_bonus[['week','season_type','defteam']]
  def_td_return_bonus.rename(columns={'defteam':'team_abbr'}, inplace=True)
  def_td_return_bonus['stat_label'] = "def_td_return_bonus"
  def_td_return_bonus = def_td_return_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  def_td_return_bonus.rename(columns={'size':'football_value'},inplace=True)
  def_td_return_bonus['fantasy_value'] = def_td_return_bonus['football_value']*6

  # defensive bonus for special teams td return
  punt_def_td_return_bonus = pbp[(pbp['return_touchdown']==1) & (pbp['play_type'].isin(['punt']))].copy()
  punt_def_td_return_bonus = punt_def_td_return_bonus[['week','season_type','defteam',]]
  punt_def_td_return_bonus.rename(columns={'defteam':'team_abbr'}, inplace=True)
  punt_def_td_return_bonus['stat_label'] = "punt_def_td_return_bonus"
  punt_def_td_return_bonus = punt_def_td_return_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  punt_def_td_return_bonus.rename(columns={'size':'football_value'},inplace=True)
  punt_def_td_return_bonus['fantasy_value'] = punt_def_td_return_bonus['football_value']*6

  # defensive bonus for special teams td return; use posteam for kickoff
  kickoff_def_td_return_bonus = pbp[(pbp['return_touchdown']==1) & (pbp['play_type'].isin(['kickoff']))].copy()
  kickoff_def_td_return_bonus = kickoff_def_td_return_bonus[['week','season_type','posteam']]
  kickoff_def_td_return_bonus.rename(columns={'posteam':'team_abbr'}, inplace=True)
  kickoff_def_td_return_bonus['stat_label'] = "kickoff_def_td_return_bonus"
  kickoff_def_td_return_bonus = kickoff_def_td_return_bonus.groupby(['week', 'season_type', 'team_abbr','stat_label'], as_index=False).size()
  kickoff_def_td_return_bonus.rename(columns={'size':'football_value'},inplace=True)
  kickoff_def_td_return_bonus['fantasy_value'] = kickoff_def_td_return_bonus['football_value']*6


  d_bonuses = pd.concat([sack_bonus,
    safety_bonus,
    fumble_recovery_bonus,
    fumble_recovery_punt_bonus,
    interception_bonus,
    block_bonus,
    def_td_return_bonus,
    punt_def_td_return_bonus,
    kickoff_def_td_return_bonus])
  
  return d_bonuses
  
  
  # # calculate points allowed for each team
  # forty_yd_plus_passing_td_qb_bonus <- rbindlist(list(
  #   unique(dt[,.(week, season_type, team_abbr = home_team, football_values = away_score)]),
  #   unique(dt[,.(week, season_type, team_abbr = away_team, football_values = home_score)])
  # ))
  # forty_yd_plus_passing_td_qb_bonus[, stat_label := "def_points_allowed"]
  # forty_yd_plus_passing_td_qb_bonus <- forty_yd_plus_passing_td_qb_bonus[,.(week, season_type, team_abbr, stat_label, football_values)]
  # def[["def_points_allowed"]] <- forty_yd_plus_passing_td_qb_bonus[, 
  #   fantasy_points := case_when(
  #     football_values == 0L ~ 10L,
  #     football_values >= 1L & football_values <= 6 ~ 7L,
  #     football_values >= 7L & football_values <= 13 ~ 4L,
  #     football_values >= 14L & football_values <= 17 ~ 1L,
  #     football_values >= 18L & football_values <= 21 ~ 0L,
  #     football_values >= 22L & football_values <= 27 ~ -1L,
  #     football_values >= 28L & football_values <= 34 ~ -4L,
  #     football_values >= 35L & football_values <= 45 ~ -7L,
  #     football_values >= 46L ~ -10L,
  #     .default = 0L
  #   )
  # ]
  
  # def <- rbindlist(def)
  # def[, position:="Defense"]
  # def[, player_id:="N/A"]
  
  # def[,.('position','week','player_id','team_abbr')]
  
  # def <- merge.data.table(def, dt_team_info_[,.(team_abbr, player_name = team_name, team_conf, team_division)], all.x = TRUE)
  
  # def <-
  #   def[, .(
  #     team_abbr,
  #     team_conf,
  #     team_division,
  #     position,
  #     week,
  #     season_type, 
  #     player_id,
  #     player_name,
  #     stat_label,
  #     football_values,
  #     fantasy_points
  #   )]
  
  # setorder(def, cols = week, position)
  
  # return

