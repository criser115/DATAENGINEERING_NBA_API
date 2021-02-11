import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

pd.options.display.max_columns = 50

DATABASE_LOCATION = "sqlite:///nba_player_stats.sqlite"


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No data downloaded. Finishing execution.")
        return False

    # Primary Key Check
    if pd.Series(df['Game_ID']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null valued found")

    return True


if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    r_games = requests.get("https://www.balldontlie.io/api/v1/games?dates[]=2021-02-09", headers=headers)
    game_data = r_games.json()

    # print(game_data)

    game_date = []
    game_ids = []
    home_team = []
    visitor_team = []
    home_team_score = []
    visitor_team_score = []

    for game in game_data["data"]:
        game_ids.append(game["id"])
        home_team.append(game["home_team"]["full_name"])
        visitor_team.append(game["visitor_team"]["full_name"])
        home_team_score.append(game["home_team_score"])
        visitor_team_score.append(game["visitor_team_score"])
        game_date.append(game["date"])

    nba_game_dict = {
        "Game_Date": game_date,
        "Game_ID": game_ids,
        "Home_Team": home_team,
        "Visitor_Team": visitor_team,
        "Home_Team_Score": home_team_score,
        "Visitor_Team_Score": visitor_team_score
    }

    nba_game_df = pd.DataFrame(nba_game_dict, columns=["Game_ID", "Home_Team", "Home_Team_Score", "Visitor_Team", "Visitor_Team_Score", "Game_Date"])

    # Validate
    if check_if_valid_data(nba_game_df):
        print("Data valid, proceed to load stage")

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('nba_player_stats.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS nba_games_played(
        game_id INT(20),
        home_team VARCHAR(200),
        home_team_score INT(20),
        visitor_team VARCHAR(200),
        visitor_team_score INT(20),
        game_date VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (game_id)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        nba_game_df.to_sql("nba_games_played", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")







