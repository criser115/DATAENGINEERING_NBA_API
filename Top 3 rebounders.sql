SELECT game_date, rebounds, player_name
FROM nba_games_played
JOIN nba_stats ON nba_games_played.game_id=nba_stats.game_id
WHERE game_date LIKE "2021-02-01%"
ORDER BY rebounds DESC 
LIMIT 3;