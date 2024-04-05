import mysql.connector

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="qwerty12345",
    database="main_data"
)
cursor = connection.cursor()

# Define the queries
queries = [
    """
    -- Drill down query into player performance metrics for a specific season.
    SELECT Player, Age, G, GS, MP_PERGAME, FG_PERGAME, FGA_PERGAME, 
           `3P_PERGAME`, `3PA_PERGAME`, `2P_PERGAME`, `2PA_PERGAME`, 
           FT_PERGAME, FTA_PERGAME, ORB_PERGAME, DRB_PERGAME, TRB_PERGAME, 
           AST_PERGAME, STL_PERGAME, BLK_PERGAME, TOV_PERGAME, PF_PERGAME, 
           PTS_PERGAME
    FROM main_data
    WHERE Season = '2021-22'
    """,
    """
    -- Roll up query rolls up player performance metrics to provide aggregate statistics based on age groups.
    SELECT 
        CASE 
            WHEN Age BETWEEN 20 AND 25 THEN '20-25'
            WHEN Age BETWEEN 26 AND 30 THEN '26-30'
            WHEN Age BETWEEN 31 AND 35 THEN '31-35'
            ELSE '36+'
        END AS Age_Group,
        COUNT(Player) AS Total_Players,
        AVG(PTS_PERGAME) AS Avg_Points_Per_Game,
        AVG(AST_PERGAME) AS Avg_Assists_Per_Game,
        AVG(DRB_PERGAME) AS Avg_Dribbles_Per_Game
    FROM main_data
    GROUP BY Age_Group
    """,
    """
    -- Slice query focuses on advanced player metrics, including efficiency ratings and percentages.
    SELECT Player, PER_ADVANCED, `TS_PERCENTAGE_ADVANCED`, `3PAr_ADVANCED`, FTr_ADVANCED, 
           `ORB_PERCENTAGE_ADVANCED`, `DRB_PERCENTAGE_ADVANCED`, `TRB_PERCENTAGE_ADVANCED`, `AST_PERCENTAGE_ADVANCED`, 
           `STL_PERCENTAGE_ADVANCED`, `BLK_PERCENTAGE_ADVANCED`, `TOV_PERCENTAGE_ADVANCED`, `USG_PERCENTAGE_ADVANCED`, 
           OWS_ADVANCED, DWS_ADVANCED, WS_ADVANCED, `WS_48_ADVANCED`, 
           OBPM_ADVANCED, DBPM_ADVANCED, BPM_ADVANCED, VORP_ADVANCED
    FROM main_data
    """,
    """
    -- This Dice query focuses on filtering the data by both age group and season.
    SELECT 
        Age,
        Season,
        COUNT(Player) AS Total_Players,
        AVG(PTS_PERGAME) AS Avg_Points_Per_Game,
        AVG(AST_PERGAME) AS Avg_Assists_Per_Game,
        AVG(DRB_PERGAME) AS Avg_Dribbles_Per_Game
    FROM main_data
    GROUP BY Age, Season
    """,
    """
    -- This dice query focuses on selecting only players who have a Player Efficiency Rating (PER_ADVANCED) greater than a certain threshold and a True Shooting Percentage (TS_PERCENTAGE_ADVANCED) greater than another threshold.
    SELECT Player, PER_ADVANCED, `TS_PERCENTAGE_ADVANCED`, `3PAr_ADVANCED`, FTr_ADVANCED, 
       `ORB_PERCENTAGE_ADVANCED`, `DRB_PERCENTAGE_ADVANCED`, `TRB_PERCENTAGE_ADVANCED`, `AST_PERCENTAGE_ADVANCED`, 
       `STL_PERCENTAGE_ADVANCED`, `BLK_PERCENTAGE_ADVANCED`, `TOV_PERCENTAGE_ADVANCED`, `USG_PERCENTAGE_ADVANCED`, 
       OWS_ADVANCED, DWS_ADVANCED, WS_ADVANCED, `WS_48_ADVANCED`, 
       OBPM_ADVANCED, DBPM_ADVANCED, BPM_ADVANCED, VORP_ADVANCED
    FROM main_data
    WHERE PER_ADVANCED > 20
    AND `TS_PERCENTAGE_ADVANCED` > 0.8;
    """,
    """
    -- Drill Down and Slice query
    SELECT 
        CASE 
            WHEN Age BETWEEN 20 AND 25 THEN '20-25'
            WHEN Age BETWEEN 26 AND 30 THEN '26-30'
            WHEN Age BETWEEN 31 AND 35 THEN '31-35'
            ELSE '36+'
        END AS Age_Group,
        Seed,
        AVG(PTS_PERGAME) AS Avg_Points_Per_Game
    FROM main_data
    WHERE GS >= 30  -- Slice by games started
    GROUP BY Age_Group, Seed
    ORDER BY Age_Group, Seed;
    """,
    """
    -- Roll Up and Slice query
    SELECT 
        Seed,
        AVG(PTS_PERGAME) AS Avg_Points_Per_Game
    FROM main_data
    WHERE GS >= 70  -- Slice games started
    GROUP BY Seed
    ORDER BY Seed;
    """,
    """
    -- Drill Down and Dice query
    SELECT 
        Age,
        Seed,
        AVG(PTS_PERGAME) AS Avg_Points_Per_Game
    FROM main_data
    WHERE GS >= 70
        AND Age BETWEEN 20 AND 30  -- Dice by Age
    GROUP BY Age, Seed
    ORDER BY Age, Seed;
    """,
    """
    -- Roll up and Dice query
    SELECT 
        CASE 
            WHEN Age BETWEEN 20 AND 25 THEN '20-25'
            WHEN Age BETWEEN 26 AND 30 THEN '26-30'
            WHEN Age BETWEEN 31 AND 35 THEN '31-35'
            ELSE '36+'
        END AS Age_Group,
        AVG(PTS_PERGAME) AS Avg_Points_Per_Game
    FROM main_data
    WHERE GS >= 10
        AND Seed = 1  -- Dice by Seed
    GROUP BY Age_Group
    ORDER BY Age_Group;
    """,
    """
    -- Iceberg query to retrieve players with the top 10% points per game (PTS_PERGAME)
    SELECT Player, PTS_PERGAME
    FROM (
        SELECT Player, PTS_PERGAME,
           NTILE(10) OVER (ORDER BY PTS_PERGAME DESC) AS Percentile
        FROM main_data
    ) AS RankedPlayers
    WHERE Percentile = 1;
    """,
    """
    -- Windowing query that calculates the average points per game (PTS_PERGAME) for each player, along with their rank based on points per game
    SELECT 
        Player,
        PTS_PERGAME,
        AVG(PTS_PERGAME) OVER (PARTITION BY Player) AS Avg_Points_Per_Game,
        RANK() OVER (ORDER BY PTS_PERGAME DESC) AS Points_Per_Game_Rank
    FROM 
        main_data;
    """,
    """
    -- Window clause to compare if player's pts_pergame above, below or equal to average
    SELECT 
        Player,
        PTS_PERGAME,
        AVG(PTS_PERGAME) OVER w AS Avg_Points_Per_Game_All_Players,
        CASE
            WHEN PTS_PERGAME > AVG(PTS_PERGAME) OVER w THEN 'Above Average'
            WHEN PTS_PERGAME < AVG(PTS_PERGAME) OVER w THEN 'Below Average'
            ELSE 'Equal to Average'
        END AS Points_Comparison
    FROM 
        main_data
    WINDOW w AS ();
    """
]

# Execute the queries
for query in queries:
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)

# Close the cursor and connection
cursor.close()
connection.close()