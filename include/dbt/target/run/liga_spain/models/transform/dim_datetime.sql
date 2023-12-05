
  
    

    create or replace table `testing-jasm`.`liga_spain`.`dim_datetime`
    
    

    OPTIONS()
    as (
      -- Create a CTE to extract date and time components
WITH datetime_cte AS (  
  SELECT DISTINCT
    Date AS datetime_id,
    CASE
      WHEN LENGTH(Date) = 10 THEN
        -- Date format: "DD/MM/YYYY HH:MM"
        PARSE_DATE('%d/%m/%Y', Date)
      WHEN LENGTH(Date) <= 8 THEN
        -- Date format: "MM/DD/YY HH:MM"
        PARSE_DATE('%d/%m/%y', Date)
      ELSE
        NULL
    END AS date_part,
  FROM `testing-jasm`.`liga_spain`.`raw_liga_spain`
  WHERE Date IS NOT NULL
)
SELECT
  datetime_id,
  date_part as datetime,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  EXTRACT(DAY FROM date_part) AS day,
  EXTRACT(DAYOFWEEK FROM date_part) AS weekday
FROM datetime_cte
    );
  