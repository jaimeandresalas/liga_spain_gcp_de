WITH TeamPoints AS (
    SELECT
        Team,
        EXTRACT(YEAR FROM MatchDate) AS Year,
        SUM(Wins * 3 + Draws * 1) AS Points,
        RANK() OVER(PARTITION BY EXTRACT(YEAR FROM MatchDate) ORDER BY SUM(Wins * 3 + Draws * 1) DESC) AS Position
    FROM (
        SELECT
            Team,
            EXTRACT(YEAR FROM MatchDate) AS Year,
            COUNT(CASE WHEN Result = 'H' THEN 1 END) AS Wins,
            COUNT(CASE WHEN Result = 'A' THEN 1 END) AS Losses,
            COUNT(CASE WHEN Result = 'D' THEN 1 END) AS Draws
        FROM (
            SELECT
                HomeTeam AS Team,
                FTR AS Result,
                PARSE_DATE('%d/%m/%y', Date) AS MatchDate
            FROM `testing-jasm`.`liga_spain`.`raw_liga_spain`
            
            UNION ALL
            
            SELECT
                AwayTeam AS Team,
                CASE
                    WHEN FTR = 'H' THEN 'A'
                    WHEN FTR = 'A' THEN 'H'
                    ELSE FTR
                END AS Result,
                PARSE_DATE('%d/%m/%y', Date) AS MatchDate
            FROM your_table_name
        ) AS CombinedResults
        GROUP BY Team, Year
    )
    GROUP BY Team, Year
)
SELECT
    Team,
    Year,
    Points,
    Position
FROM TeamPoints
ORDER BY Year, Position