/********************************************************************
Cumulative Table Design in Trino (with Starburst and Tabular)
********************************************************************/
-- DROP TABLE jkenney0501.nba_players

-- create my own schema for any DDL needed
CREATE SCHEMA jkenney0501

-- get a feel of the data set used and its attributes
SELECT * 
FROM bootcamp.nba_player_seasons 
WHERE player_name = 'Michael Jordan'


-- Step 1, DDL structure with complex data types.
-- to create a dimension using a cumulative table design, we need to capture all attributes of the data that does not change and use that to create the table

CREATE TABLE jkenney0501.nba_players(
player_name VARCHAR,
height VARCHAR,
college VARCHAR,
country VARCHAR,
draft_year VARCHAR,  -- "undrafted" is a value attribute is not INT, same below
draft_round VARCHAR,
draft_number VARCHAR, 
-- the rest of the data is at the season grain abd goes to an array
-- array is created as a row
seasons ARRAY(ROW(
		 season INTEGER,
		 age INTEGER,
		 weight INTEGER,
		 gp INTEGER,
		 pts DOUBLE,
	         reb DOUBLE,
		 ast DOUBLE)),
is_active BOOLEAN,
years_since_last_active INTEGER,
current_season INTEGER
)

-- must be formatted as PARQUET and use a partition
WITH (
	format ='PARQUET',
	partitioning = ARRAY['current_season']
)




-- CREATE cumulative table using CTE's and FULL OUTER JOIN
-- insert the data into the table previously created
INSERT INTO jkenney0501.nba_players

-- create cumulative table using CTE's and FULL OUTER JOIN

WITH last_season AS(
SELECT * 
FROM jkenney0501.nba_players
WHERE current_season = 1995 -- to cumulate, change + 1
),
this_season AS(
SELECT * 
FROM bootcamp.nba_player_seasons
WHERE season = 1996  -- to cumulate, change + 1 current year then run query again
)

-- final query FOR THE INSERT INTO new table: joins both last and current with OUTER JOIN
-- we want to COALESECE all the values that are not changing before the array (which does change in the CASE as seasons accumulate)
SELECT 
COALESCE(ls.player_name, ts.player_name) AS player_name,
COALESCE(ls.height, ts.height) AS height,
COALESCE(ls.college, ts.college) AS college,
COALESCE(ls.country, ts.country) AS country,
COALESCE(ls.draft_year, ts.draft_year) AS draft_year,
COALESCE(ls.draft_round, ts.draft_round) AS draft_round,
COALESCE(ls.draft_number, ts.draft_number) AS draft_number,
-- this determines if a player is newly active, no longer active or both previousluy and currently season active
CASE 
  WHEN ts.season IS NULL THEN ls.seasons -- if this season is null then populate last season data
  WHEN ts.season IS NOT NULL AND ls.seasons IS NULL THEN ARRAY[ROW(ts.season, ts.age, ts.weight, ts.gp, ts.pts, ts.ast, ts.reb)]
  WHEN ts.season IS NOT NULL AND ls.seasons IS NOT NULL THEN ARRAY[ROW(ts.season, ts.age, ts.weight, ts.gp, ts.pts, ts.ast, ts.reb)] || ls.seasons 
END AS seasons,
ts.season IS NOT NULL AS is_active, -- if not null then it is current/active else the player is not active any loner
CASE WHEN ts.season IS NOT NULL THEN 0 ELSE years_since_last_active + 1 END AS years_since_last_active,
COALESCE(ts.season, ls.current_season +1) AS current_season
FROM last_season ls
FULL OUTER JOIN this_season ts ON ls.player_name = ts.player_name



-- test query for jordan
SELECT *
FROM jkenney0501.nba_players
WHERE current_season = 1995  
AND player_name = 'Michael Jordan'


-- see the change in 2000 because he retired for 3 years in 1997, the years_since_last_active will now show 3 and is_active will be false.
SELECT *
FROM jkenney0501.nba_players
WHERE current_season = 2002
AND player_name = 'Michael Jordan'


-- what if you want to get the data in the array?
-- by cross joining and unesting! This returns the player data in the array for those years.
-- CROSS JOIN isn't the best method, and we also don't want to use  GROUP BY to get SUMS for analytical purposes. 
-- The point of cumulative design is to avoid shuffles! We can use REDUCE to get a total.
SELECT 
player_name,
t.*
FROM jkenney0501.nba_players
CROSS JOIN UNNEST(seasons) AS t
WHERE current_season = 2002
AND player_name = 'Michael Jordan'


-- get total points over career for Jordan by processing the aaray.
-- accumulator is a temp var that holds each row, r is for row and s maps it over. It loops over the array and starts at 0.
SELECT 
player_name,
REDUCE(seasons, 0, (cumulator, r)-> cumulator + r.pts * r.gp, s-> s) AS total_points
FROM jkenney0501.nba_players
WHERE current_season = 2002
AND player_name = 'Michael Jordan'


-- to see stats for each attribute compare to bootcamp.nba_players to see how the PARQUET and partiton affect the byte size
SHOW STATS FOR (SELECT *
FROM jkenney0501.nba_players
WHERE current_season = 2002)



















