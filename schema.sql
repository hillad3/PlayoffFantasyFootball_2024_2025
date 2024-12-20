DROP TABLE IF EXISTS nfl_stats;

CREATE TABLE nfl_stats (
    roster_index INTEGER NOT NULL,
    position_code TEXT NOT NULL,
    fantasy_owner TEXT NOT NULL,
    fantasy_team TEXT NOT NULL,
    season_type TEXT NOT NULL,
    week INTEGER NOT NULL,
    team_abbr TEXT NOT NULL,
    team_conf TEXT NOT NULL,
    team_division TEXT NOT NULL,
    position_type TEXT NOT NULL,
    position TEXT NOT NULL,
    player_id TEXT NOT NULL,
    player_name TEXT NOT NULL,
    lookup_string TEXT NOT NULL,
    subsequently_traded TEXT NOT NULL,
    stat_label TEXT NOT NULL,
    football_value INTEGER NOT NULL,
    fantasy_points INTEGER NOT NULL
);