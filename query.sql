WITH user_install_date AS (
   SELECT DISTINCT user_id,
                   MIN(event_date_parsed) AS install_date
   FROM mydatabase.analytics_projectid.transformed_event_data
   WHERE device_web_info_hostname = 'game.app.com'
     AND event_name IN ('first_launch_app', 'launch_app', 'game_start')
   GROUP BY user_id
   ORDER BY install_date
),
  
user_data_with_lifedays AS (
   SELECT DISTINCT t.user_id AS user_id,
                   uid.install_date AS install_date,
                   t.event_date_parsed AS event_date,
                   DATE_DIFF(t.event_date_parsed, uid.install_date, DAY) AS life_day
   FROM mydatabase.analytics_projectid.transformed_event_data AS t
            LEFT JOIN user_install_date AS uid
                      ON t.user_id = uid.user_id
   WHERE t.device_web_info_hostname = 'game.app.com'
     AND t.event_name IN ('first_launch_app', 'launch_app', 'game_start')
),
  
user_max_lifeday AS (
   SELECT user_id, MAX(life_day) AS max_life_day
   FROM user_data_with_lifedays
   GROUP BY user_id
),
  
user_data_with_lifedays_maxdays AS (
   SELECT a.user_id, a.install_date, a.event_date, a.life_day, m.max_life_day
   FROM user_data_with_lifedays AS a
            LEFT JOIN user_max_lifeday AS m
                      ON a.user_id = m.user_id
   ORDER BY user_id, install_date, event_date
),
  
pivot_table_source AS (
   SELECT install_date,
          life_day,
          COUNT(DISTINCT CASE WHEN life_day = 0 THEN user_id END) AS day_0,
          COUNT(DISTINCT CASE WHEN life_day = 1 THEN user_id END) AS day_1,
          COUNT(DISTINCT CASE WHEN life_day = 2 THEN user_id END) AS day_2,
          COUNT(DISTINCT CASE WHEN life_day = 3 THEN user_id END) AS day_3,
          COUNT(DISTINCT CASE WHEN life_day = 4 THEN user_id END) AS day_4,
          COUNT(DISTINCT CASE WHEN life_day = 5 THEN user_id END) AS day_5,
          COUNT(DISTINCT CASE WHEN life_day = 6 THEN user_id END) AS day_6,
          COUNT(DISTINCT CASE WHEN life_day = 7 THEN user_id END) AS day_7,
          COUNT(DISTINCT CASE WHEN life_day = 8 THEN user_id END) AS day_8,
          COUNT(DISTINCT CASE WHEN life_day = 9 THEN user_id END) AS day_9,
          COUNT(DISTINCT CASE WHEN life_day = 10 THEN user_id END) AS day_10,
          COUNT(DISTINCT CASE WHEN life_day = 11 THEN user_id END) AS day_11,
          COUNT(DISTINCT CASE WHEN life_day = 12 THEN user_id END) AS day_12,
          COUNT(DISTINCT CASE WHEN life_day = 13 THEN user_id END) AS day_13,
          COUNT(DISTINCT CASE WHEN life_day = 14 THEN user_id END) AS day_14,
          COUNT(DISTINCT CASE WHEN life_day = 15 THEN user_id END) AS day_15,
          COUNT(DISTINCT CASE WHEN life_day = 16 THEN user_id END) AS day_16,
          COUNT(DISTINCT CASE WHEN life_day = 17 THEN user_id END) AS day_17,
          COUNT(DISTINCT CASE WHEN life_day = 18 THEN user_id END) AS day_18,
          COUNT(DISTINCT CASE WHEN life_day = 19 THEN user_id END) AS day_19,
          COUNT(DISTINCT CASE WHEN life_day = 20 THEN user_id END) AS day_20,
          COUNT(DISTINCT CASE WHEN life_day = 21 THEN user_id END) AS day_21,
          COUNT(DISTINCT CASE WHEN life_day = 22 THEN user_id END) AS day_22,
          COUNT(DISTINCT CASE WHEN life_day = 23 THEN user_id END) AS day_23,
          COUNT(DISTINCT CASE WHEN life_day = 24 THEN user_id END) AS day_24,
          COUNT(DISTINCT CASE WHEN life_day = 25 THEN user_id END) AS day_25,
          COUNT(DISTINCT CASE WHEN life_day = 26 THEN user_id END) AS day_26,
          COUNT(DISTINCT CASE WHEN life_day = 27 THEN user_id END) AS day_27,
          COUNT(DISTINCT CASE WHEN life_day = 28 THEN user_id END) AS day_28,
          COUNT(DISTINCT CASE WHEN life_day = 29 THEN user_id END) AS day_29,
          COUNT(DISTINCT CASE WHEN life_day = 30 THEN user_id END) AS day_30,
          COUNT(DISTINCT CASE WHEN life_day = 31 THEN user_id END) AS day_31
   FROM user_data_with_lifedays_maxdays
   GROUP BY install_date, life_day
   ORDER BY install_date, life_day
),
  
pivot_table AS (
  SELECT install_date,
      	MAX(day_0) AS D0,
      	MAX(day_1) AS D1,
      	MAX(day_2) AS D2,
      	MAX(day_3) AS D3,
      	MAX(day_4) AS D4,
      	MAX(day_5) AS D5,
      	MAX(day_6) AS D6,
      	MAX(day_7) AS D7,
      	MAX(day_8) AS D8,
      	MAX(day_9) AS D9,
      	MAX(day_10) AS D10,
      	MAX(day_11) AS D11,
      	MAX(day_12) AS D12,
      	MAX(day_13) AS D13,
      	MAX(day_14) AS D14,
      	MAX(day_15) AS D15,
      	MAX(day_16) AS D16,
      	MAX(day_17) AS D17,
      	MAX(day_18) AS D18,
      	MAX(day_19) AS D19,
      	MAX(day_20) AS D20,
      	MAX(day_21) AS D21,
      	MAX(day_22) AS D22,
      	MAX(day_23) AS D23,
      	MAX(day_24) AS D24,
      	MAX(day_25) AS D25,
      	MAX(day_26) AS D26,
      	MAX(day_27) AS D27,
      	MAX(day_28) AS D28,
      	MAX(day_29) AS D29,
      	MAX(day_30) AS D30,
      	MAX(day_31) AS D31
  FROM pivot_table_source
  GROUP BY install_date ORDER BY install_date DESC
)

SELECT install_date,
      CAST(D0 AS float64) / NULLIF(D0, 0) * 100 AS D0,
       ROUND((CAST(D1 AS float64) / NULLIF(D0, 0) * 100), 1) AS D1,
       ROUND((CAST(D2 AS float64) / NULLIF(D0, 0) * 100), 1) AS D2,
       ROUND((CAST(D3 AS float64) / NULLIF(D0, 0) * 100), 1) AS D3,
       ROUND((CAST(D4 AS float64) / NULLIF(D0, 0) * 100), 1) AS D4,
       ROUND((CAST(D5 AS float64) / NULLIF(D0, 0) * 100), 1) AS D5,
       ROUND((CAST(D6 AS float64) / NULLIF(D0, 0) * 100), 1) AS D6,
       ROUND((CAST(D7 AS float64) / NULLIF(D0, 0) * 100), 1) AS D7,
       ROUND((CAST(D8 AS float64) / NULLIF(D0, 0) * 100), 1) AS D8,
       ROUND((CAST(D9 AS float64) / NULLIF(D0, 0) * 100), 1) AS D9,
       ROUND((CAST(D10 AS float64) / NULLIF(D0, 0) * 100), 1) AS D10,
       ROUND((CAST(D11 AS float64) / NULLIF(D0, 0) * 100), 1) AS D11,
       ROUND((CAST(D12 AS float64) / NULLIF(D0, 0) * 100), 1) AS D12,
       ROUND((CAST(D13 AS float64) / NULLIF(D0, 0) * 100), 1) AS D13,
       ROUND((CAST(D14 AS float64) / NULLIF(D0, 0) * 100), 1) AS D14,
       ROUND((CAST(D15 AS float64) / NULLIF(D0, 0) * 100), 1) AS D15,
       ROUND((CAST(D16 AS float64) / NULLIF(D0, 0) * 100), 1) AS D16,
       ROUND((CAST(D17 AS float64) / NULLIF(D0, 0) * 100), 1) AS D17,
       ROUND((CAST(D18 AS float64) / NULLIF(D0, 0) * 100), 1) AS D18,
       ROUND((CAST(D19 AS float64) / NULLIF(D0, 0) * 100), 1) AS D19,
       ROUND((CAST(D20 AS float64) / NULLIF(D0, 0) * 100), 1) AS D20,
       ROUND((CAST(D21 AS float64) / NULLIF(D0, 0) * 100), 1) AS D21,
       ROUND((CAST(D22 AS float64) / NULLIF(D0, 0) * 100), 1) AS D22,
       ROUND((CAST(D23 AS float64) / NULLIF(D0, 0) * 100), 1) AS D23,
       ROUND((CAST(D24 AS float64) / NULLIF(D0, 0) * 100), 1) AS D24,
       ROUND((CAST(D25 AS float64) / NULLIF(D0, 0) * 100), 1) AS D25,
       ROUND((CAST(D26 AS float64) / NULLIF(D0, 0) * 100), 1) AS D26,
       ROUND((CAST(D27 AS float64) / NULLIF(D0, 0) * 100), 1) AS D27,
       ROUND((CAST(D28 AS float64) / NULLIF(D0, 0) * 100), 1) AS D28,
       ROUND((CAST(D29 AS float64) / NULLIF(D0, 0) * 100), 1) AS D29,
       ROUND((CAST(D30 AS float64) / NULLIF(D0, 0) * 100), 1) AS D30,
       ROUND((CAST(D31 AS float64) / NULLIF(D0, 0) * 100), 1) AS D31
FROM pivot_table;
