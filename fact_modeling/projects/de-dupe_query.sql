/******************************************************************************
q1

Write a query to de-duplicate the nba_game_details table from the day 1 lab of 
the fact modeling week 2 so there are no duplicate values.

You should de-dupe based on the combination of game_id, team_id and player_id, 
since a player cannot have more than 1 entry per game.

Feel free to take the first value here.
*******************************************************************************/
-- EDA
SELECT 
game_id,
team_id,
player_id,
ROW_NUM() OVER(PARTITION BY game_id, player_id BY ORDER BY team_id ) AS row_indicator -- doesnt work well, use row num
FROM bootcamp.nba_game_details
ORDER BY 1,2,3
LIMIT 100

-- get dup counts for eah unqiue player
SELECT 
game_id,
team_id,
player_id,
COUNT(1) AS counts
FROM bootcamp.nba_game_details
GROUP BY 1,2,3
ORDER BY 4 DESC


-- using row number to identofy the dupllciate values fro each player on each team for each game
-- the three columns are partitioned whic gives them a unque row
-- the row_number windows function will count the rows ineach partition
-- if there is a duplicate value, it will then be > 1
-- we only want the rows that = 1

-- check for those > 1 to see dups
WITH ranked_data AS (
    SELECT
        game_id,
        team_id,
        player_id,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY team_id) AS row_num
    FROM
        bootcamp.nba_game_details
)
SELECT *
FROM
    ranked_data
WHERE
    row_num <> 1


-- change the operator in the WHERE clause to = 1 to get de-duped data
-- this can be inserted into a table usin gthe INSERT INTO <table name> statement before the CTE
WITH ranked_data AS (
    SELECT
        game_id,
        team_id,
        player_id,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY team_id) AS row_num
    FROM
        bootcamp.nba_game_details
)
SELECT *
FROM
    ranked_data
WHERE
    row_num = 1
