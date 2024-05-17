-- deduping and retention considerations


-- create a fact table, prefix with fct
-- ids go first 
-- use DIM for dimemsions of the fact
-- m_ for measures

CREATE OR REPLACE TABLE jkenney0501.fct_nba_game_details(
game_id BIGINT,
team_id BIGINT,
player_id BIGINT,
dim_team_abbreviation VARCHAR,
dim_player_name VARCHAR,
dim_start_position VARCHAR,
dim_did_not_dress BOOLEAN,
dim_not_with_team BOOLEAN,
m_seconds_played INTEGER,
m_field_goals_made DOUBLE, --to avoid integer div
m_field_goals_attempted DOUBLE,
m_three_pointers_made DOUBLE,
m_three_pointers_attempted DOUBLE,
m_free_throws_made DOUBLE,
m_free_throws_attempted DOUBLE,
m_offensive_rebounds DOUBLE,
m_defensive_rebounds DOUBLE,
m_rebounds DOUBLE,
m_assists DOUBLE,
m_steals DOUBLE,
m_blocks DOUBLE,
m_turnovers DOUBLE,
m_personal_fouls DOUBLE,
m_points DOUBLE,
m_plus_minus DOUBLE,
dim_game_date DATE,
dim_season INTEGER,
dim_team_did_win BOOLEAN
)
WITH(
  format = 'parquet',
  partitioning = ARRAY['dim_season']
)

-- create query for schema 
-- ideally, this is incrementally loaded in at the enterprose level but one big load is used here.
INSERT INTO jkenney0501.fct_nba_game_details

WITH games AS(
SELECT 
game_id,
game_date_est,
season,
home_team_wins,
home_team_id,
visitor_team_id
FROM bootcamp.nba_games
)
SELECT 
g.game_id,
gd.team_id,
gd.player_id,
gd.team_abbreviation AS dim_team_abbreviation,
gd.player_name AS dim_player_name,
gd.start_position AS dim_start_position,
gd.comment LIKE '%DND%' AS dim_did_not_dress,
gd.comment LIKE '%NWT%' AS dim_not_with_team,
CASE WHEN CARDINALITY(SPLIT(MIN,':')) > 1 
    THEN CAST(CAST(SPLIT(gd.min, ':')[1] AS DOUBLE)  * 60 + CAST(SPLIT(gd.min, ':')[2] AS DOUBLE) AS INTEGER) 
    ELSE 
        CAST(gd.min AS INTEGER) 
END AS m_seconds_played,
CAST(gd.fgm AS DOUBLE) AS m_field_goals_made,
CAST(gd.fga AS DOUBLE) AS m_field_goals_attempted,
CAST(gd.fg3m AS DOUBLE) AS m_three_pointers_made,
CAST(gd.fg3a AS DOUBLE) AS m_three_pointers_attempted,
CAST(gd.ftm AS DOUBLE) AS m_free_throws_made,
CAST(gd.fta AS DOUBLE) AS m_free_throws_attempted,
CAST(gd.oreb AS DOUBLE) AS m_offensive_rebounds,
CAST(gd.dreb AS DOUBLE) AS m_defensive_rebounds,
CAST(gd.reb AS DOUBLE) AS m_rebounds,
CAST(gd.ast AS DOUBLE) AS m_assists,
CAST(gd.stl AS DOUBLE) AS m_steals,
CAST(gd.blk AS DOUBLE) AS m_blocks,
CAST(gd.to AS DOUBLE) AS m_turnovers,
CAST(gd.pf AS DOUBLE) AS m_personal_fouls,
CAST(gd.pts AS DOUBLE) AS m_points,
CAST(gd.plus_minus AS DOUBLE) AS m_plus_minus,
g.game_date_est AS dim_game_date,
g.season AS dim_season,
CASE WHEN gd.team_id = g.home_team_id THEN home_team_wins = 1 ELSE home_team_wins = 0 END AS dim_team_did_win
FROM games AS g 
JOIN bootcamp.nba_game_details AS gd ON g.game_id = gd.game_id


