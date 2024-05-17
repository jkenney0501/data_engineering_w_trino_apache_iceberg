/*******************************************************************
Windows Functions

LEAD -shows the data a following
LAG - shows the data preceding specififed by ROWS clause

RANK - ranks the rows in the window based on the partion.
********************************************************************/

-- USING LAG to identify a consectuvie streak of 20 point games and DESNE_RANK to get top 10 with consecutive 20 point seasons

WITH lagged AS(
SELECT 
player_name, 
season,
pts,
LAG(pts, 1) OVER(
                PARTITION BY player_name 
                ORDER BY season) AS points_last_season
FROM bootcamp.nba_player_seasons 
-- WHERE player_name IN ('LeBron James', 'Allen Iverson')
ORDER BY season
),

did_change AS(
SELECT *,
CASE WHEN pts >= 20 AND points_last_season >= 20 THEN 1 ELSE 0 END AS points_stayed_above_20
FROM lagged
),

identifier AS(
SELECT *,
SUM(points_stayed_above_20) 
  OVER(PARTITION BY player_name ORDER BY season) AS  consecutive_seasons_over_20
FROM did_change
),

aggregated AS(
SELECT player_name,
COUNT(CASE WHEN pts >= 20 THEN 1  END) AS consecutive_seasons_over_20,
MIN(season) AS season_start,
MAX(season) AS season_end
FROM identifier
GROUP BY player_name
),
-- USING DENSE RANK AS TO NOT SKIP SEQUENCE AND GET TOP 10 CONSECUTIVE SEASONS
ranked AS(
SELECT *,
DENSE_RANK() OVER(ORDER BY consecutive_seasons_over_20 DESC) AS ranking
FROM aggregated
)

SELECT *
FROM ranked
WHERE ranking <= 10















