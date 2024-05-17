-- formatting and aggregating facts: reduced facts - lab 3
-- 1. ******* daily aggregate layer *******
CREATE OR REPLACE TABLE jkenney0501.daily_web_metrics(
    user_id BIGINT,
    metric_name VARCHAR,
    metric_value BIGINT,
    date DATE
)
-- format as parquet for efficient storage
WITH(
    format = 'parquet',
    partitioning = ARRAY['metric_name', 'date']
)

-- insert the data
INSERT INTO jkenney0501.daily_web_metrics

SELECT 
user_id,
'visited_home_page' AS metric_name,
COUNT(CASE WHEN url = '/' THEN 1 END) AS metric_value,
CAST(event_time AS DATE) AS date
FROM bootcamp.web_events
GROUP BY user_id, CAST(event_time AS DATE)


-- ****** Test Query *****
-- check query aggregates before insert
SELECT
date,
metric_name,
SUM(metric_value) AS metric_value
FROM jkenney0501.daily_web_metrics
GROUP BY date, metric_name
ORDER BY  metric_value DESC



-- 2. ******** Monthly **********
-- turn into monthly array metrics
CREATE OR REPLACE TABLE jkenney0501.monthly_arr_web_metrics(
    user_id BIGINT,
    metric_name VARCHAR,
    metric_array ARRAY(INTEGER),
    month_start VARCHAR
)
-- format storage and partition
WITH(
    format = 'parquet',
    partitioning = ARRAY['metric_name', 'month_start']
)


-- test query before insert
INSERT INTO jkenney0501.monthly_arr_web_metrics

WITH yesterday AS(
    SELECT *
    FROM jkenney0501.monthly_arr_web_metrics
    WHERE month_start = '2023-08-01'
),
today AS (
    SELECT *
    FROM jkenney0501.daily_web_metrics
    WHERE date = DATE('2023-08-01')
)

SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.metric_name, y.metric_name) AS metric_name,
    ARRAY[t.metric_value] metric_array,
    '2023-08-01' AS month_start
FROM today AS t
FULL OUTER JOIN yesterday AS y ON t.user_id = y.user_id 
AND t.metric_name = y.metric_name