-- SCD Type 2 
-- Modeling SCD type 2 
-- using is_active column as an indicator and current_season as a patition key

-- SHOW TABLES FROM jkenney0501
-- jkenney0501.nba_players , jkenney0501.nba_players_scd
-- describe jkenney0501.nba_players_scd

SELECT *
FROM jkenney0501.nba_players
WHERE current_season = 2001


-- create the scd table
CREATE TABLE jkenney0501.nba_players_scd(
player_name VARCHAR,
is_active BOOLEAN,
start_season INTEGER,
end_season INTEGER,
current_season INTEGER
)

-- format and call out parition
WITH (
format = 'PARQUET',
partitioning = ARRAY['current_season']
)

-- **** one load SCD that loads all at once rather than increment *****

-- 1st CTE captures activity and inactivty by displaying if the player was active last season and is currently active.
-- REMEMBER TO TUNCATE or INSERT OVERWRITE FOR new data or you will have duplicate values and a whole bunch of data you dont need.
-- 1278 rows

INSERT INTO jkenney0501.nba_players_scd
WITH lagged AS(
SELECT 
player_name,
CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
CASE WHEN LAG(is_active, 1) OVER(PARTITION BY player_name ORDER BY current_season) THEN 1 ELSE 0 END AS is_active_last_season,
current_season
FROM jkenney0501.nba_players
WHERE current_season <= 2001 -- doing this for incremental load later on 
),

-- identifies a streak of active last season and current
streaked AS (
SELECT *,
SUM(CASE WHEN is_active <> is_active_last_season THEN 1 ELSE 0 END) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_identifier
FROM lagged
-- WHERE player_name = 'Ben Davis' -- for test purposes
)

-- test scd with Jordan
SELECT 
player_name,
MAX(is_active) = 1 AS is_active,
MIN(current_season) AS start_season,
MAX(current_season) AS end_season,
2001 AS current_season -- hard code for inc load
FROM streaked
-- WHERE player_name = 'Michael Jordan'
GROUP BY player_name, streak_identifier





-- *************** load 2002 Data Incrementally ************************

-- last season scd for incremental load

INSERT INTO jkenney0501.nba_players_scd
WITH last_season_scd AS(
SELECT *
FROM jkenney0501.nba_players_scd
WHERE current_season = 2001 -- incremntal load starts at 200
),
-- current season scd increments one year
current_season_scd AS(
SELECT *
FROM jkenney0501.nba_players
WHERE current_season = 2002 -- incremntal load starts at 200
),
combined AS(
SELECT 
COALESCE(ls.player_name,cs.player_name) AS player_name,
COALESCE(ls.start_season,cs.current_season) AS start_season,
COALESCE(ls.end_season,cs.current_season) AS end_season,
CASE 
  WHEN ls.is_active <> cs.is_active THEN 1 
  WHEN ls.is_active = cs.is_active THEN 0
END AS did_change,
ls.is_active AS is_active_last_season,
cs.is_active AS is_active_current_season,
2002 AS current_season
FROM last_season_scd AS ls
FULL OUTER JOIN current_season_scd AS cs ON ls.player_name = cs.player_name AND ls.end_season + 1 = cs.current_season
),
changes AS (
    SELECT
      player_name,
      current_season,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
              is_active_last_season,
              start_season,
              end_season + 1
            ) AS ROW(
              is_active boolean,
              start_season integer,
              end_season integer
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(is_active_last_season, start_season, end_season) AS ROW(
              is_active boolean,
              start_season integer,
              end_season integer
            )
          ),
          CAST(
            ROW(
              is_active_current_season,
              current_season,
              current_season
            ) AS ROW(
              is_active boolean,
              start_season integer,
              end_season integer
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(is_active_last_season, is_active_current_season),
              start_season,
              end_season
            ) AS ROW(
              is_active boolean,
              start_season integer,
              end_season integer
            )
          )
        ]
      END AS change_array
    FROM
      combined
  )
SELECT
  player_name,
  arr.is_active,
  arr.start_season,
  arr.end_season,
  current_season
FROM
  changes
  CROSS JOIN UNNEST (change_array) AS arr















