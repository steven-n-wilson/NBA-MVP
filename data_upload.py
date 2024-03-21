import csv
import mysql.connector
import pandas

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="qwerty12345",
    database="main_data"
)

# Create a cursor
cursor = connection.cursor()

# Read CSV and insert data into MySQL table, skipping the first column
with open('data.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header
    for row in reader:
        # Skip the first column (index 0) by slicing the row
        cursor.execute("""
            INSERT INTO main_data (
                Player, 
                Age, 
                G, 
                GS, 
                MP_PERGAME, 
                FG_PERGAME, 
                FGA_PERGAME, 
                FG_PERCENTAGE_PERGAME, 
                3P_PERGAME, 
                3PA_PERGAME, 
                3P_PERCENTAGE_PERGAME, 
                2P_PERGAME, 
                2PA_PERGAME, 
                2P_PERCENTAGE_PERGAME, 
                eFG_PERCENTAGE_PERGAME, 
                FT_PERGAME, 
                FTA_PERGAME, 
                FT_PERCENTAGE_PERGAME, 
                ORB_PERGAME, 
                DRB_PERGAME, 
                TRB_PERGAME, 
                AST_PERGAME, 
                STL_PERGAME, 
                BLK_PERGAME, 
                TOV_PERGAME, 
                PF_PERGAME, 
                PTS_PERGAME, 
                PER_ADVANCED, 
                TS_PERCENTAGE_ADVANCED, 
                3PAr_ADVANCED, 
                FTr_ADVANCED, 
                ORB_PERCENTAGE_ADVANCED, 
                DRB_PERCENTAGE_ADVANCED, 
                TRB_PERCENTAGE_ADVANCED, 
                AST_PERCENTAGE_ADVANCED, 
                STL_PERCENTAGE_ADVANCED, 
                BLK_PERCENTAGE_ADVANCED, 
                TOV_PERCENTAGE_ADVANCED, 
                USG_PERCENTAGE_ADVANCED, 
                OWS_ADVANCED, 
                DWS_ADVANCED, 
                WS_ADVANCED, 
                WS_48_ADVANCED, 
                OBPM_ADVANCED, 
                DBPM_ADVANCED, 
                BPM_ADVANCED, 
                VORP_ADVANCED, 
                MP_TOTAL, 
                FG_TOTAL, 
                FGA_TOTAL, 
                FG_PERCENTAGE_TOTAL, 
                3P_TOTAL, 
                3PA_TOTAL, 
                3P_PERCENTAGE_TOTAL, 
                2P_TOTAL, 
                2PA_TOTAL, 
                2P_PERCENTAGE_TOTAL, 
                eFG_PERCENTAGE_TOTAL, 
                FT_TOTAL, 
                FTA_TOTAL, 
                FT_PERCENTAGE_TOTAL, 
                ORB_TOTAL, 
                DRB_TOTAL, 
                TRB_TOTAL, 
                AST_TOTAL, 
                STL_TOTAL, 
                BLK_TOTAL, 
                TOV_TOTAL, 
                PF_TOTAL, 
                PTS_TOTAL, 
                Seed, 
                PCT, 
                MVP_Rank, 
                MVP_Votes_Share, 
                Season
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, row[1:])

# Commit changes and close connection
connection.commit()
cursor.close()
connection.close()