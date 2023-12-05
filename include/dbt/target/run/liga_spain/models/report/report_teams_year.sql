
  
    

    create or replace table `testing-jasm`.`liga_spain`.`report_teams_year`
    
    

    OPTIONS()
    as (
      SELECT 
    Year,
    Team,
    SUM(Wins) as TotalWins,
    SUM(Losses) as TotalLosses,
    SUM(Draws) as TotalDraws
FROM (
    SELECT 
        dt.year as Year,
        raw.HomeTeam as Team,
        CASE WHEN raw.FTR = 'H' THEN 1 ELSE 0 END as Wins,
        CASE WHEN raw.FTR = 'A' THEN 1 ELSE 0 END as Losses,
        CASE WHEN raw.FTR = 'D' THEN 1 ELSE 0 END as Draws
    FROM 
        `testing-jasm`.`liga_spain`.`raw_liga_spain` as raw
        join `testing-jasm`.`liga_spain`.`dim_datetime` as dt
        on raw.Date = dt.datetime_id
    UNION ALL
    SELECT 
        dt.year as Year,
        raw.AwayTeam as Team,
        CASE WHEN raw.FTR = 'A' THEN 1 ELSE 0 END as Wins,
        CASE WHEN raw.FTR = 'H' THEN 1 ELSE 0 END as Losses,
        CASE WHEN raw.FTR = 'D' THEN 1 ELSE 0 END as Draws
    FROM 
        `testing-jasm`.`liga_spain`.`raw_liga_spain` as raw
        join `testing-jasm`.`liga_spain`.`dim_datetime` as dt
        on raw.Date = dt.datetime_id
) 
GROUP BY 
    Year, 
    Team
ORDER BY
    Year, 
    Team
    );
  