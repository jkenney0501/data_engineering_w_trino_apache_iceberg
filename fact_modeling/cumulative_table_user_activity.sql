/*******************************************************************************************************************************
day 2 lab

Cumulative table that captures user activity
and their corresponding activty status. 

Captured below are the intricacies of handling date calculations and utilizing bitwise operations to represent user activity. 
The lab captures the presenter encountering and troubleshooting various challenges, providing valuable insights and explanations.
***********************************************************************************************************************************/
-- idea is to create a table that  captures user id and has an array of dates active
-- this tracks the uswer activity over a period of time

-- EDA
SELECT *
FROM bootcamp.web_events
WHERE DATE_TRUNC('day', event_time) = DATE('2023-01-01')


-- create table to track user activity
CREATE OR REPLACE TABLE jkenney0501.web_users_cumulated (
user_id BIGINT,
dates_active ARRAY(DATE),
curr_date DATE
)
WITH(
  format = 'parquet',
  partitioning = ARRAY['curr_date']
)


-- create yesterday and today tables to cumulate data
-- repeat the process for 7 days to cumulate a weeks data
INSERT INTO jkenney0501.web_users_cumulated

WITH yesterday AS(
SELECT * 
FROM jkenney0501.web_users_cumulated
WHERE curr_date = DATE('2022-12-31') -- change to cumulate
),
today AS(
SELECT 
user_id,
CAST(DATE_TRUNC('day', event_time) AS DATE) AS event_date,
COUNT(1) AS user_count
FROM bootcamp.web_events
WHERE DATE_TRUNC('day', event_time) = DATE('2023-01-01') -- change to cumulate
GROUP BY user_id, CAST(DATE_TRUNC('day', event_time) AS DATE)
)
SELECT 
COALESCE(y.user_id, t.user_id) AS user_id,
CASE WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
  ELSE ARRAY[t.event_date] -- if user was active yesterday then put that event in the array with todays event,
  -- if yesterdays event is not there then put todays in the array and repeat the process tomorrow.
END AS dates_active,
DATE('2023-01-01') AS date -- change to cumulate
FROM yesterday AS y
FULL OUTER JOIN today AS t ON y.user_id = t.user_id



-- check results for most rcent day to see cumulative active days in array for users
SELECT *
FROM jkenney0501.web_users_cumulated
WHERE curr_date = DATE('2023-01-07')



-- take the list of dates and convert to see how many days user was active in array
WITH today AS(
SELECT *
FROM jkenney0501.web_users_cumulated
WHERE curr_date = DATE('2023-01-07')
),
date_list_int AS (
select 
user_id,
  CAST(SUM(
        CASE WHEN CONTAINS(dates_active, sequence_date) THEN
        POW(2, 30 - DATE_DIFF('day', sequence_date, curr_date))
        ELSE 0 
    END
  ) AS BIGINT) AS history_int
FROM today
CROSS JOIN UNNEST (SEQUENCE(DATE('2023-01-01'), DATE('2023-01-07'))) AS t(sequence_date)
GROUP BY user_id
)

SELECT *,
history_int,
TO_BASE(history_int, 2) AS hist_in_binary -- represented in a binary formmat that shows if user was active or not with 1 as active and 0 as not active
BIT_COUNT(history_int, 32) AS num_days_active-- this represents a summed count of days vs the binary
FROM date_list_int
