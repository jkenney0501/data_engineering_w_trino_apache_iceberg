/***************************************************************************************************
This assignment involves working with the actor_films dataset. Your task is to construct a series of
 SQL queries and table definitions that will allow us to model the actor_films dataset in a way that 
 facilitates efficient analysis. This involves creating new tables, defining data types, and writing queries 
 to populate these tables with data from the actor_films dataset.

Dataset Overview

The actor_films dataset contains the following fields:

    actor: The name of the actor.
    actor_id: A unique identifier for each actor.
    film: The name of the film.
    year: The year the film was released.
    votes: The number of votes the film received.
    rating: The rating of the film.
    film_id: A unique identifier for each film.

The primary key for this dataset is (actor_id, film_id).

 *****************************************************************************************************/

 -- EDA of data set
 -- SHOW CREATE TABLE bootcamp.actor_films
 -- SHOW TABLES FROM jkenney0501
 -- SELECT * FROM bootcamp.actor_films LIMIT 50

 -- create table actors (query 1)
CREATE TABLE jkenney0501.actors(
actor VARCHAR NOT NULL,
actor_id VARCHAR NOT NULL,
films ARRAY(
    ROW( -- row creates and array that contain data that can accumulate/even change as time passes, aka attributes of the film
        film VARCHAR,
        year INTEGER,
        votes INTEGER,
        rating DOUBLE,
        film_id VARCHAR
        )),
quality_class VARCHAR, -- will rate the quality of the movie wiht a CASE statement
is_active BOOLEAN, 
current_year INTEGER
 )

-- define the storage and partition
 WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
 )


-- insert cumulative records 1 year at a time 1995-03
INSERT INTO jkenney0501.actors

WITH last_year AS (
SELECT * 
FROM jkenney0501.actors
WHERE current_year = 1994
),
current_year AS (
SELECT *
FROM bootcamp.actor_films
WHERE year = 1995
)

SELECT -- coalesce is used to capture all data since is active can be false in cumulative design
COALESCE(ly.actor, cy.actor) AS actor,
COALESCE(ly.actor_id, cy.actor_id) AS actor_id ,
CASE -- capture all 3 possible states from the full join and create arrays
    WHEN cy.year IS NULL THEN ly.films 
    WHEN cy.year IS NOT NULL AND ly.films IS NULL THEN 
        ARRAY_AGG(
            ROW(
                cy.film, 
                cy.year, 
                cy.votes, 
                cy.rating, 
                cy.film_id)
        )
    WHEN cy.year IS NOT NULL AND ly.films IS NOT NULL THEN 
        ARRAY_AGG(
            ROW(
                cy.film, 
                cy.year, 
                cy.votes, 
                cy.rating, 
                cy.film_id)
            ) || ly.films -- concats previous year films to current year in array
END AS films,
CASE
    WHEN AVG(cy.rating)  > 8 THEN 'star'
    WHEN AVG(cy.rating)  > 7 THEN 'good'
    WHEN AVG(cy.rating)  > 6 THEN 'average'
    ELSE 'bad'
END AS quality_class, -- this is wierd to me but only solution I could figure out that worked
cy.year IS NOT NULL AS is_active,
COALESCE(cy.year, ly.current_year + 1) AS current_year
FROM last_year AS ly
FULL OUTER JOIN current_year AS cy ON ly.actor_id = cy.actor_id


--******************************** V2 *************************************
-- this works for q2
INSERT INTO jkenney0501.actors

WITH last_year AS (
SELECT * 
FROM jkenney0501.actors
WHERE current_year = 1999
),
current_year AS (
SELECT 
actor,
actor_id,
ARRAY_AGG( -- using array_agg elinimates the need for long case in final select
        ROW(
            film,
            year, 
            votes, 
            rating, 
            film_id
            )
        ) AS films,
CASE -- avg is for actors current year on a rolling basis
    WHEN AVG(rating)  > 8 THEN 'star'
    WHEN AVG(rating)  > 7 THEN 'good'
    WHEN AVG(rating)  > 6 THEN 'average'
    ELSE 'bad' 
END AS quality_class,
year       
FROM bootcamp.actor_films
WHERE year = 2000
GROUP BY actor, actor_id, year
)

SELECT -- coalesce is used to capture all data since is active can be false in cumulative design
COALESCE(ly.actor, cy.actor) AS actor,
COALESCE(ly.actor_id, cy.actor_id) AS actor_id ,
COALESCE(cy.films, ly.films)  AS films,
COALESCE(cy.quality_class, ly.quality_class) AS quality_class, -- this is wierd to me but only solution I could figure out that worked
cy.year IS NOT NULL AS is_active,
COALESCE(cy.year, ly.current_year + 1) AS current_year
FROM last_year AS ly
FULL OUTER JOIN current_year AS cy ON ly.actor_id = cy.actor_id


-- AND ly.current_year = cy.year -1
-- test query
/*
SELECT * 
FROM jkenney0501.actors
WHERE current_year = 2003

SELECT * 
FROM jkenney0501.actors
WHERE current_year = 2003
AND is_active = false
*/

/************************************************************
Write a DDL statement to create an actors_history_scd table that 
tracks the following fields for each actor in the actors table:

quality_class
is_active
start_date
end_date

Note that this table should be appropriately modeled as a Type 2 
Slowly Changing Dimension Table (start_date and end_date).

fields on actors table: 
actor	actor_id	films	quality_class	is_active	current_year
*************************************************************/
-- query 3, create actors_history_scd
CREATE TABLE jkenney0501.actors_history_scd(
    actor VARCHAR NOT NULL,
    quality_class VARCHAR NOT NULL,
    is_active BOOLEAN,
    start_date INTEGER NOT NULL,
    end_date INTEGER NOT NULL,
    current_year INTEGER NOT NULL
)

-- format and call out parition
WITH (
format = 'PARQUET',
partitioning = ARRAY['current_year']
)



-- q4 - query 4, create one load/batch backfill scd
INSERT INTO jkenney0501.actors_history_scd

WITH
  lagged AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      CASE
        WHEN is_active THEN 1
        ELSE 0
      END AS is_active,
      CASE
        WHEN LAG(is_active, 1) OVER (
          PARTITION BY
            actor_id
          ORDER BY
            current_year
        ) THEN 1
        ELSE 0
      END AS active_last_year,
      current_year
    FROM
      jkenney0501.actors
    WHERE
      current_year <= 2005
  ),
  streak AS (
    SELECT
      *,
      -- create new variable to identify continuity or lackthereof
      SUM(
        CASE
          WHEN is_active <> active_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor
        ORDER BY
          current_year
      ) AS consecutive_streak
    FROM
      lagged
  )
SELECT
  actor,
  quality_class,
  MAX(is_active) = 1 AS is_active, -- this year status
  MIN(current_year) AS start_year, -- start year status
  MAX(current_year) AS end_year, -- end year status
  2005 AS current_year
  --current_year
FROM
  streak
GROUP BY
  actor,
  quality_class,
  consecutive_streak




-- query 5, Incremental load - start at 2005, load 2006 as next increment
INSERT INTO jkenney0501.actors_history_scd
WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      jkenney0501.actors_history_scd
    WHERE
      current_year = 2005
  ),
  current_year_scd AS (
    SELECT
      *
    FROM
      jkenney0501.actors
    WHERE
      current_year = 2006
  ),
  combined AS (
    SELECT
      COALESCE(ly.actor, cy.actor) AS actor,
      COALESCE(ly.quality_class, cy.quality_class) AS quality_class,
      COALESCE(ly.start_date, cy.current_year) AS start_date,
      COALESCE(ly.end_date, cy.current_year) AS end_date,
      CASE
        WHEN ly.is_active <> cy.is_active THEN 1
        WHEN ly.is_active = cy.is_active THEN 0
      END AS did_change,
      ly.is_active AS is_active_last_year,
      cy.is_active AS is_active_this_year,
      2006 AS current_year
    FROM
      last_year_scd AS ly
      FULL OUTER JOIN current_year_scd AS cy ON ly.actor = cy.actor
      AND ly.end_date + 1 = cy.current_year
  ),
changes AS(
SELECT
  actor,
  quality_class,
  current_year,
  CASE
    WHEN did_change = 0 THEN ARRAY[
      CAST(
        ROW(is_active_last_year, start_date, end_date + 1) AS ROW(
          is_active BOOLEAN,
          start_date INTEGER,
          end_date INTEGER
        )
      )
    ]
    WHEN did_change = 1 THEN ARRAY[
      CAST(
        ROW(is_active_last_year, start_date, end_date) AS ROW(
          is_active BOOLEAN,
          start_date INTEGER,
          end_date INTEGER
        )
      ),
      CAST(
        ROW(is_active_this_year, current_year, current_year) AS ROW(
          is_active BOOLEAN,
          start_date INTEGER,
          end_date INTEGER
        )
      )
    ]
    WHEN did_change IS NULL THEN ARRAY[
      CAST(
        ROW(
          COALESCE(is_active_last_year, is_active_this_year),
          start_date,
          end_date
        ) AS ROW(
          is_active BOOLEAN,
          start_date INTEGER,
          end_date INTEGER
        )
      )
    ]
  END AS changed_array
FROM
  combined
)

SELECT 
actor,
quality_class,
a.is_active,
a.start_date,
a.end_date,
current_year
FROM changes
CROSS JOIN UNNEST(changed_array) AS a