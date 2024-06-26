/***********************************************************************************
Dimensional Modeling Overview:

Understanding idempotent pipelines and dimensional modeling

- Idempotent pl: meaning translates to "always produces the same result, regardless of when its ran." Whether its dev or backfill, its always the same result.
Its similar to a mathematical function, when given the same input, it will produce the same result. Same input data set should produce same output dataset.


- Why non-idempotent pl's are problematic
-backfilling causes inconsistencies between the old and restated data.
- very hard to troubleshoot bugs
- unit testing cannot replicate the production behavior
- silent failures


- What make your pl's not idempotent?
1. INSERT INTO without TRUNCATE will double the input. 
- USE MERGE or INSERT OVERWRITE every time!
2.Using a start date > w/o a corresponding end date <
3. Not using a full set partition sensors (pipeline might run when there is no/partial data-running with incomplete x (input))
4. Not using depends_on_past for cumulative pipelines.

- slowly changing dimensions: DIMs that change over time (example: employees, products, addresses, etc)
(caution: singular snapshots are not idempotent!- backfilling fails to capture the history)
(you can use daily partitioned snapshots- snap every day) best option is to incorporate CDC with SCD's

- SCD 0: really is not a SCD. It doesn't change - i.e. birthday, type of animal etc.

- SCD1: uses only the latest value, no backfilling! Captures no history at all. Don't use, PL is not idempotent. Filter with is_current for 1 row of current.

- SCD2: has start date and end date. end for current row is null. ONly SCD that is purely idempotent.

- SCD3: only captures original and current. Drawback is you lose history. Partially idempotent. 

Type 0 + 2 are idempotent.
Type 1 is not, if you backfill you will get the dim as it is now not as it was then.
Type 3 is not idempotent. Impossible to see history.


SCD Loading:
Load entire history in one query (this example does just this and that's fine for the 1st load but not for constant ELT/ETL daily process)
- inefficient but nimble
- 1 query and you're done

Inclemently load the data after the SCD is generated
- has the same depends_on_past constraint
- efficient but cumbersome

***********************************************************************************/

-- Creates idempotent type 2 SCD using TRINO and Iceberg
WITH all_data AS(
SELECT player_name, 
exploded_season,
MAX(season = exploded_season) AS is_active
FROM bootcamp.nba_player_seasons
CROSS JOIN UNNEST (sequence(1996,2002)) as t(exploded_season)
-- WHERE player_name = 'Michael Jordan'
GROUP BY player_name, 
         exploded_season
ORDER BY exploded_season
),
change_identifed AS(
SELECT *,
LAG(is_active,1) OVER(PARTITION BY player_name ORDER BY exploded_season) AS is_active_last_season,
CASE WHEN is_active = LAG(is_active,1) OVER(PARTITION BY player_name ORDER BY exploded_season) THEN 0 ELSE 1 END AS did_change
FROM all_data
),
identified AS (
SELECT *,
SUM(did_change) OVER(PARTITION BY player_name ORDER BY exploded_season) AS streak_identifier
FROM change_identifed
) 

SELECT 
player_name,
is_active,
MIN(exploded_season) AS start_season,
MAX(exploded_season) AS end_season,
MAX(exploded_season) = 2021 AS is_current
FROM identified
GROUP BY player_name,
        is_active,
        streak_identifier