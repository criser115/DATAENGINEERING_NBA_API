import pandas as pd
import requests

DATABASE_LOCATION = "sqlite:///nba_player_stats.sqlite"


if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    r = requests.get("https://www.balldontlie.io/api/v1/stats", headers=headers)

    data = r.json()

    print(data)
