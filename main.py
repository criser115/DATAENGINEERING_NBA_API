import pandas as pd
import requests


DATABASE_LOCATION = "sqlite:///nba_player_stats.sqlite"


if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    r = requests.get("https://www.balldontlie.io/api/v1/stats?seasons[]=2020", headers=headers)

    data = r.json()
    #print(data)

    players = []
    teams = []
    pts = []
    game_id = []

    for item in data["data"]:
        players.append(item["player"]["first_name"] + " " + item["player"]["last_name"])
        teams.append(item["team"]["full_name"])
        pts.append(item["pts"])
        game_id.append(item["game"]["id"])

    nba_dict = {
        "NBA_Players": players,
        "NBA_Teams": teams,
        "Points_Scored": pts,
        "Game_ID": game_id
    }

    nba_df = pd.DataFrame(nba_dict, columns=["NBA_Players", "NBA_Teams", "Points_Scored", "Game_ID"])
    print(nba_df.nlargest(10, columns=['Points_Scored']))



