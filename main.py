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


def check_if_valid_game(df: pd.DataFrame) -> bool:
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


def check_if_valid_stat(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No data downloaded. Finishing execution.")
        return False

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null valued found")

    return True


if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    r_games = requests.get("https://www.balldontlie.io/api/v1/games?dates[]=2021-02-12&per_page=100", headers=headers)
    r_stats = requests.get("https://www.balldontlie.io/api/v1/stats?dates[]=2021-02-12&per_page=100", headers=headers)
    game_data = r_games.json()
    stat_data = r_stats.json()

    # print(game_data)
    # print(stat_data)

    # Game stat lists
    nba_players = []
    nba_teams = []
    pts = []
    reb = []
    assists = []
    player_game_ids = []

    # Add stats to lists
    for stat in stat_data["data"]:
        nba_players.append(stat["player"]["first_name"] + " " + stat["player"]["last_name"])
        nba_teams.append(stat["team"]["name"])
        pts.append(stat["pts"])
        reb.append(stat["reb"])
        assists.append(stat["ast"])
        player_game_ids.append(stat["game"]["id"])

    # add stat data to dictionary
    nba_stat_dict = {
        "player_name": nba_players,
        "nba_team": nba_teams,
        "points_scored": pts,
        "rebounds": reb,
        "assists": assists,
        "game_id": player_game_ids
    }

    # Add stat data to dataframe, limit is 100 per data vendor limits
    nba_stat_df = pd.DataFrame(nba_stat_dict,
                               columns=["player_name", "nba_team", "points_scored", "rebounds", "assists", "game_id"])

    # print(nba_stat_df)

    # Game data lists
    game_date = []
    game_ids = []
    home_team = []
    visitor_team = []
    home_team_score = []
    visitor_team_score = []

    # Add game data to lists
    for game in game_data["data"]:
        game_ids.append(game["id"])
        home_team.append(game["home_team"]["full_name"])
        visitor_team.append(game["visitor_team"]["full_name"])
        home_team_score.append(game["home_team_score"])
        visitor_team_score.append(game["visitor_team_score"])
        game_date.append(game["date"])

    # Add game data to dictionary
    nba_game_dict = {
        "Game_Date": game_date,
        "Game_ID": game_ids,
        "Home_Team": home_team,
        "Visitor_Team": visitor_team,
        "Home_Team_Score": home_team_score,
        "Visitor_Team_Score": visitor_team_score
    }

    # Add game data to dataframe
    nba_game_df = pd.DataFrame(nba_game_dict,
                               columns=["Game_ID", "Home_Team", "Home_Team_Score", "Visitor_Team", "Visitor_Team_Score",
                                        "Game_Date"])

    # Validate
    if check_if_valid_game(nba_game_df):
        print("Game data valid, proceed to load stage")

    if check_if_valid_stat(nba_stat_df):
        print("Stat data valid, proceed to load stage")

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('nba_player_stats.sqlite')
    conn.execute("PRAGMA foreign_keys = on")
    cursor = conn.cursor()

    # Query command for nba_games_played table
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

    # Query command for nba_stats table
    sql_stat_query = """
        CREATE TABLE IF NOT EXISTS nba_stats(                                   
            player_name VARCHAR(200),
            nba_team VARCHAR(200),
            points_scored INT(20),
            rebounds INT(20),
            assists INT(20),
            game_id INT(20),            
            CONSTRAINT fk_nba_games_played
                FOREIGN KEY (game_id)
                REFERENCES nba_games_played(game_id)                      
        )
        """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        nba_game_df.to_sql("nba_games_played", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    nba_stat_df.to_sql("nba_stats", engine, index=False, if_exists='append')

    conn.close()
    print("Close database successfully")







