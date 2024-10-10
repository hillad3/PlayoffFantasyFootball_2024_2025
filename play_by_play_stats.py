# %%
import pandas as pd

# %%
def make(file : str, 
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

def offense_bonuses(pbp : pd.DataFrame) -> pd.DataFrame:

  # # create a list to hold each unique fantasy football points dataset
  # bonus <- list()
  
  # offensive bonus for touchdown with pass over 40 yards for qb
  # bonus_40yd_pass_td_qb_bonus = 
  tmp = pbp[(pbp['pass_touchdown']==1) & (pbp['passing_yards']>=40)].copy()
  tmp.rename(columns={'posteam':'team_abbr','passer_player_name':'player_name','passer_player_id':'player_id'}, inplace=True)
  tmp = tmp.groupby(['week', 'season_type', 'team_abbr', 'player_name', 'player_id']).agg(
    stat_label = '40yd_pass_td_qb_bonus',
    football_value = ('player_id','count'),
    fantasy_point = ('player_id','count')*2
  )
  #   stat_label = '40yd_pass_td_qb_bonus',
  #   football_value = 'count'
  #   fantasy_points = 'count'*2
  # )
  return tmp
  
  # bonus[["40yd_pass_td_receiver_bonus"]] <- dt[
  #   pass_touchdown == 1L & passing_yards >= 40, 
  #   by = .(week, season_type, team_abbr = posteam, player = receiver_player_name, player_id = receiver_player_id),
  #   list(stat_label = "40yd_pass_td_receiver_bonus", football_values = .N, fantasy_points=.N*2L)
  # ]
  
  # # offensive bonus for touchdown with rush over 40 yards for qb
  # bonus[["40yd_rush_td_bonus"]] <- dt[
  #   rush_touchdown == 1L & rushing_yards >= 40, 
  #   by = .(week, season_type, team_abbr = posteam, player = rusher_player_name, player_id = rusher_player_id),
  #   list(stat_label = "40yd_rush_td_bonus", football_values = .N, fantasy_points=.N*2L)
  # ]
  
  # # player bonus for returning a td
  # # only for normal possession plays by the opposite team (ie. pass or rush)
  # # in a kickoff, the receiving team is listed as the posteam
  # # in a punt, the receiving team is listed as the defteam
  # tmp <- rbindlist(list(
  #   dt[
  #     play_type == "kickoff" & !is.na(kickoff_returner_player_name) & return_touchdown == 1L & return_yards >= 40, 
  #     by = .(week,
  #            season_type, 
  #            team_abbr = posteam,
  #            player = kickoff_returner_player_name,
  #            player_id = kickoff_returner_player_id),
  #     list(stat_label = "40yd_return_td_bonus", football_values = .N, fantasy_points=.N*2L)
  #   ],
  #   dt[
  #     play_type == "punt" & !is.na(punt_returner_player_name) & return_touchdown == 1L & return_yards >= 40, 
  #     by = .(week,
  #            season_type, 
  #            team_abbr = defteam,
  #            player = punt_returner_player_name,
  #            player_id = punt_returner_player_id),
  #     list(stat_label = "40yd_return_td_bonus", football_values = .N, fantasy_points=.N*2L)
  #   ]
  # )) 
  # bonus[["40yd_return_td_bonus"]] <- tmp[,by = .(week,season_type,team_abbr,player,player_id,stat_label),
  #     list(football_values = sum(football_values), fantasy_points = sum(fantasy_points))]
  
  # bonus <- rbindlist(bonus)
  
  # bonus <- merge.data.table(bonus, dt_rosters_[,.(player_id, player_name, team_abbr, position)], all.x = TRUE, by = c("player_id", "team_abbr"))
  
  # if(any(is.na(bonus$position))){
  #   print(paste0("There were ", length(bonus$position[is.na(bonus$position)]), " rows removed because of NAs in position"))
  #   bonus <- bonus[!is.na(position)]
  # }

  # # rename Fullback to Running Back
  # bonus <- rbindlist(list(
  #   bonus[position=="FB"][,position:="RB"],
  #   bonus[position!="FB"]
  # ))
  
  # if(any(!(bonus$position %in% c('QB', 'RB', 'WR', 'TE')))){
  #   print(paste0("There were ", dim(bonus[!(position %in% c('QB', 'RB', 'FB', 'WR', 'TE'))])[1], 
  #                " rows removed because of position is out of scope"))
  #   bonus <- bonus[position %in% c('QB', 'RB', 'WR', 'TE')]
  # }
  
  # bonus <- merge.data.table(bonus, dt_team_info_[,.(team_abbr, team_conf, team_division)], all.x = TRUE, by = c("team_abbr"))
  
  # bonus <-
  #   bonus[, .(
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
  
  # setorder(bonus, cols = week, position)
  
  # return(bonus)


def d_stats():

  # # create a list to hold each unique fantasy football points dataset
  # def <- list()
  
  # ## defensive bonus for sacks
  # # If you want to exclude sack where the QB got back to the line of scrimmage, then add filter 
  # # condition of yards_gained < 0L. There are some instances where the sack_player is not recorded 
  # # but there was still a sack recorded (seemingly if a fumble happens in the same play)
  # def[["def_sack"]] <- dt[
  #   sack == 1L,
  #   by = .(week, season_type, team_abbr = defteam),
  #   list(stat_label="def_sack", football_values = .N, fantasy_points = .N*1L)
  # ]
  
  # # defensive bonus for safeties
  # def[["def_safety"]] <- dt[
  #   safety == 1L & !is.na(safety_player_id),
  #   by = .(week, season_type, team_abbr = defteam),
  #   list(stat_label="def_safety", football_values = .N, fantasy_points = .N*1L)
  # ]
  
  # # defensive bonus for fumble recovery
  # def[["def_fumble_recovery"]] <- dt[
  #   fumble == 1L & fumble_lost == 1L & play_type != "punt",
  #   by = .(week, season_type, team_abbr = defteam),
  #   list(stat_label="def_fumble_recovery", football_values = .N, fantasy_points = .N*2L)
  # ]
  
  # # defensive bonus for fumble recovery for a punt
  # # punts start with the receiving team listed as defteam, so those may need special consideration
  # def[["def_fumble_recovery_punt"]] <- dt[
  #   fumble == 1L & fumble_lost == 1L & play_type == "punt",
  #   by = .(week, season_type, team_abbr = posteam),
  #   list(stat_label="def_fumble_recovery_punt", football_values = .N, fantasy_points = .N*2L)
  # ]
  
  # # defensive bonus for interceptions
  # def[["def_interception"]] <- dt[
  #   interception == 1L,
  #   by = .(week, season_type, team_abbr = defteam),
  #   list(stat_label="def_interception", football_values = .N, fantasy_points = .N*2L)
  # ]
  
  # # def bonus for blocks on punt, fg or extra point
  # def[["def_block"]] <- dt[
  #   !is.na(blocked_player_name),
  #   by = .(week, season_type, team_abbr = defteam),
  #   list(stat_label="def_block", football_values = .N, fantasy_points = .N*2L)
  # ]
  
  # # def bonus for def td for any reason or cause (block, fumble, interception, etc)
  # # only for normal possession plays by the opposite team (ie. pass or rush)
  # def[["def_td"]] <- dt[
  #   return_touchdown == 1L & play_type %in% c("pass", "run"),
  #   by = .(week, season_type, team_abbr = defteam),
  #   list(stat_label="def_td", football_values = .N, fantasy_points = .N*6L)
  # ]
  
  # # special teams bonus for a return td
  # # in a kickoff, the kicking team is listed as the defteam
  # def[["def_kickoff_return_td"]] <- dt[
  #   return_touchdown == 1L & play_type %in% c("kickoff"),
  #   by = .(week, season_type, team_abbr = posteam),
  #   list(stat_label="def_kickoff_return_td", football_values = .N, fantasy_points = .N*6L)
  # ]
  
  # # special teams bonus for a return td
  # # in a punt, the receiving team is listed as the defteam
  # def[["def_punt_return_td"]] <- dt[
  #   return_touchdown == 1L & play_type %in% c("punt"),
  #   by = .(week, season_type, team_abbr = posteam),
  #   list(stat_label="def_punt_return_td", football_values = .N, fantasy_points = .N*6L)
  # ]
  
  # # calculate points allowed for each team
  # tmp <- rbindlist(list(
  #   unique(dt[,.(week, season_type, team_abbr = home_team, football_values = away_score)]),
  #   unique(dt[,.(week, season_type, team_abbr = away_team, football_values = home_score)])
  # ))
  # tmp[, stat_label := "def_points_allowed"]
  # tmp <- tmp[,.(week, season_type, team_abbr, stat_label, football_values)]
  # def[["def_points_allowed"]] <- tmp[, 
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

  return None