from nba_api.stats.endpoints import teams, ShotChartDetail, LeagueGameLog, BoxScorePlayerTrackV3, BoxScoreAdvancedV3, playbyplayv3, BoxScoreTraditionalV3, TeamGameLogs, BoxScoreAdvancedV2, teamgamelog
import pandas as pd
import time
import os
from datetime import datetime
from functools import wraps
from typing import Any
import requests
from datetime import datetime, timedelta


# Retrieve all game IDs for a given season and season type.
def get_game_ids(season, season_type='Regular Season'):
    game_log = LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
    game_ids = game_log['GAME_ID'].unique().tolist()
    return game_ids

# Get the current season and format it correctly "2013-14"
def get_current_season():
    current_date = datetime.now()
    year = current_date.year
    if current_date.month < 10:
        season = str(year - 1) + '-' + str(year)[2:]
    else:
        season = str(year) + '-' + str(year + 1)[2:]
    return season

# Wrapper for retrying missed api calls in get data function
def retry_decorator(max_retries: int = 3, initial_delay: float = 32.0, backoff_factor: float = 2) -> callable:
    def decorator(func: callable) -> callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            last_exception = None
            
            for retry_count in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if retry_count < max_retries:
                        time.sleep(delay)
                        delay *= backoff_factor

            raise last_exception
            
        return wrapper
    return decorator

# Get data from the enpoints for a list of game ids 
def get_data(game_ids, nba_teams):
    play_by_play_data = []
    shotchart_data = []
    playertracking_data = []
    advanced_data = []
    box_data = []
    team_advanced_data = []
    team_game_logs_data = []
    
    max_retries = 4
    initial_delay = 32.0

    # Code to work with retry wrapper for each endpoint 
    @retry_decorator(max_retries=max_retries, initial_delay=initial_delay)
    def fetch_box_score(game_id):
        return BoxScoreTraditionalV3(game_id=game_id).get_data_frames()[0]

    @retry_decorator(max_retries=max_retries, initial_delay=initial_delay)
    def fetch_play_by_play(game_id):
        return playbyplayv3.PlayByPlayV3(game_id=game_id).get_data_frames()[0]

    @retry_decorator(max_retries=max_retries, initial_delay=initial_delay)
    def fetch_shot_chart(game_id):
        return ShotChartDetail(
            game_id_nullable=game_id,
            team_id=0,
            player_id=0,
            context_measure_simple='FGA'
        ).get_data_frames()[0]

    @retry_decorator(max_retries=max_retries, initial_delay=initial_delay)
    def fetch_player_tracking(game_id):
        return BoxScorePlayerTrackV3(game_id=game_id).get_data_frames()[0]

    @retry_decorator(max_retries=max_retries, initial_delay=initial_delay)
    def fetch_advanced_boxscore(game_id):
        return BoxScoreAdvancedV3(game_id=game_id).get_data_frames()[0]

    @retry_decorator(max_retries=max_retries, initial_delay=initial_delay)
    def fetch_advanced_boxscore_team(game_id):
        return BoxScoreAdvancedV2(game_id=game_id).get_data_frames()[1]

    @retry_decorator(max_retries=max_retries, initial_delay=initial_delay)
    def fetch_team_game_logs(team_id):
        return TeamGameLogs( team_id_nullable= team_id , season_nullable=season).get_data_frames()[0]

    # keep track of missed games, remove from list and dont report as fetched
    game_list = game_ids.copy()

    # iterate through game ids and fetch data
    for game_id in game_ids:
        try:
            # Standard box
            try:
                box = fetch_box_score(game_id)
                box['GAME_ID'] = game_id
                box_data.append(box)
            except Exception as e:
                with open(log_file, 'a') as f:
                    f.write(f"Failed box traditional for {game_id}: {e}\n")
                if game_id in game_list:
                    game_list.remove(game_id)
                time.sleep(32)

            # Play by play
            try:
                play_by_play = fetch_play_by_play(game_id)
                play_by_play['GAME_ID'] = game_id
                play_by_play_data.append(play_by_play)
            except Exception as e:
                with open(log_file, 'a') as f:
                    f.write(f"Failed PlayByPlayV2 for {game_id}: {e}\n")
                if game_id in game_list:
                    game_list.remove(game_id)    
                time.sleep(32)

            # ShotChartDetail
            try:
                shotchart = fetch_shot_chart(game_id)
                shotchart['GAME_ID'] = game_id
                shotchart_data.append(shotchart)
            except Exception as e:
                with open(log_file, 'a') as f:
                    f.write(f"Failed ShotChartDetail for {game_id}: {e}\n")
                if game_id in game_list:
                    game_list.remove(game_id)
                time.sleep(32)

            # BoxScorePlayerTrackV3
            try:
                player_tracking = fetch_player_tracking(game_id)
                player_tracking['GAME_ID'] = game_id
                playertracking_data.append(player_tracking)
            except Exception as e:
                with open(log_file, 'a') as f:
                    f.write(f"Failed BoxScorePlayerTrackV3 for {game_id}: {e}\n")
                if game_id in game_list:
                    game_list.remove(game_id)
                time.sleep(32)

            # BoxScoreAdvancedV3
            try:
                advanced_boxscore = fetch_advanced_boxscore(game_id)
                advanced_boxscore['GAME_ID'] = game_id
                advanced_data.append(advanced_boxscore)
            except Exception as e:
                with open(log_file, 'a') as f:
                    f.write(f"Failed BoxScoreAdvancedV3 for {game_id}: {e}\n")
                if game_id in game_list:
                    game_list.remove(game_id)
                time.sleep(32)
            
            # BoxScoreAdvancedV2 (Team)
            try:
                team_advanced_boxscore = fetch_advanced_boxscore_team(game_id=game_id)
                team_advanced_boxscore['GAME_ID'] = game_id
                team_advanced_data.append(team_advanced_boxscore)
            except Exception as e:
                with open(log_file, 'a') as f:
                    f.write(f"Failed BoxScoreAdvancedV2 for {game_id}: {e}\n")
                if game_id in game_list:
                    game_list.remove(game_id)
                time.sleep(32)


            # Rate limiting
            time.sleep(0.7)
            
        except Exception as e:
            print(f"Unexpected error for game {game_id}: {e}")
            time.sleep(32)

    for team in nba_teams:
        try:
            game_log = fetch_team_game_logs(team = team['TEAM_ID'])
            team_game_logs_data.append(game_log)
        except Exception as e:
            with open(log_file, 'a') as f:
                f.write(f"Failed team_game_logs for {team['TEAM_NAME']}: {e}\n")
            if game_id in game_list:
                game_list.remove(game_id)
            time.sleep(32)
        # Rate limiting
            time.sleep(0.7)

        

    # Concat dataframes only if data exists
    shotchart_df = pd.concat(shotchart_data, ignore_index=True) if shotchart_data else pd.DataFrame()
    playertracking_df = pd.concat(playertracking_data, ignore_index=True) if playertracking_data else pd.DataFrame()
    advanced_df = pd.concat(advanced_data, ignore_index=True) if advanced_data else pd.DataFrame()
    play_by_play_df = pd.concat(play_by_play_data, ignore_index=True) if play_by_play_data else pd.DataFrame()
    traditional_box_df = pd.concat(box_data, ignore_index=True) if box_data else pd.DataFrame()
    team_advanced_df = pd.concat(team_advanced_data,ignore_index=True) if team_advanced_data else pd.DataFrame()
    team_game_log_df = pd.concat(team_game_logs_data,ignore_index=True) if team_game_logs_data else pd.DataFrame()

    return shotchart_df, playertracking_df, advanced_df, play_by_play_df, traditional_box_df, team_advanced_df, game_list, team_game_log_df

# Check if files exist, create or update
def update_csv(file_path, new_df):
    if not os.path.exists(file_path):
        new_df.to_csv(file_path, index=False)
    else:
        existing_df = pd.read_csv(file_path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(inplace=True)
        combined_df.to_csv(file_path, index=False)
        



# Retrieve the current NBA season.
season =  get_current_season()

data_path = "/data/NBA/byYearData/"
log_path = "/data/NBA/logs/"
finished_data_path = "/data/NBA/combinedData/"

#get logs
log_file = f"{data_path}{season}_log.txt"

with open(log_file, 'a') as f:
    f.write(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Check against a txt file of previously checked games
checked_games_file = f'{data_path}checked_games{season}.txt'
if os.path.exists(checked_games_file):
    with open(checked_games_file, 'r') as f:
        checked_games = [line.strip() for line in f.readlines()]
        
else:
    checked_games = []

# Get a list of game IDs for the current season.
game_ids  = get_game_ids(season = season)

# Filter out games that have already been checked
filtered_game_ids  = [game_id for game_id in game_ids if game_id not in checked_games]

# Get teams 
nba_teams = teams.get_teams()

# get data for new games 
shotchart_df, playertracking_df,advanced_box_df,play_by_play_df,traditional_box_df, team_advanced_df, fetched_games, team_game_log_df = get_data(filtered_game_ids, nba_teams)

# Append new checked games to the txt file
with open(checked_games_file, 'a') as f:
    for game_id in fetched_games:
        f.write(str(game_id) + '\n')

#names
shotchart_file = f"{data_path}{season}_shotchart_data.csv"
playertracking_file = f"{data_path}{season}_playertracking_data.csv"
advanced_box_file = f"{data_path}{season}_advanced_box_data.csv"
play_by_play_file = f'{data_path}{season}_play_by_play.csv'
traditional_box_file = f"{data_path}{season}_box_data.csv"
team_advanced_box_file = f"{data_path}{season}_team_advanced.csv"
team_game_logs_file = f"{data_path}{season}_team_game_logs.csv"



# update the csv's
update_csv(shotchart_file, shotchart_df)
update_csv(playertracking_file, playertracking_df)
update_csv(advanced_box_file, advanced_box_df)
update_csv(play_by_play_file, play_by_play_df)
update_csv(traditional_box_file, traditional_box_df)
update_csv(team_advanced_box_file, team_advanced_df)
update_csv(team_game_logs_file,team_game_log_df)

##### Get the schedule and find the next games to predict
url = "https://stats.nba.com/stats/scheduleleaguev2"

params = {
    "Season": season,
    "LeagueID": "00",
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Connection": "keep-alive",
}

time.sleep(30)  #make sure itsnot to close to other requests
response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    game_dates = data['leagueSchedule']['gameDates']
    all_games = []

    for date_info in game_dates:
        game_date = date_info['gameDate']
        games = date_info['games']
        for game in games:
            game['gameDate'] = game_date  # Add the date to each game record
            all_games.append(game)

    df = pd.DataFrame(all_games)

    # convert date and get the next weeks games 
    df['gameDate'] = pd.to_datetime(df['gameDate'], format='%m/%d/%Y %H:%M:%S')
    current_date = datetime.utcnow()
    next_week = current_date + timedelta(weeks=1)
    next_week_games = df[(df['gameDate'] >= current_date) & (df['gameDate'] <= next_week)]

    # get the team ID's for the next weeks games 
    home = pd.json_normalize(next_week_games['homeTeam'])
    away = pd.json_normalize(next_week_games['awayTeam'])

    Upcoming_matchups = pd.concat(
        [home[['teamId']].rename(columns={'teamId': 'home_teamId'}),
        away[['teamId']].rename(columns={'teamId': 'away_teamId'})],
        axis=1
    )   
    Upcoming_matchups_file = f"{data_path}{season}_upcoming_matchups.csv"
    update_csv(Upcoming_matchups_file,Upcoming_matchups)

else:
    with open(log_file, 'a') as f:
        f.write(f"Sechdule Request failed with status code: {response.status_code}")

# Combine data from all seasons into dataframes for analysis
data_types = {
    '_shotchart_data.csv': 'shotchart_df_all',
    '_playertracking_data.csv': 'playertracking_df_all',
    '_advanced_box_data.csv': 'advanced_box_df_all',
    '_play_by_play.csv': 'play_by_play_df_all',
    '_box_data.csv': 'traditional_box_df_all',
    '_team_advanced.csv': 'team_advanced_df_all',
    '_team_game_logs.csv': 'team_game_logs_df_all'
}

dataframes = {value: pd.DataFrame() for value in data_types.values()}
season_files = [f for f in os.listdir(data_path) if any(f.endswith(key) for key in data_types)]

for season_file in season_files:
    for file_suffix, df_name in data_types.items():
        if season_file.endswith(file_suffix):
            file_path = os.path.join(data_path, season_file)
            temp_df = pd.read_csv(file_path)
            dataframes[df_name] = pd.concat([dataframes[df_name], temp_df], ignore_index=True)

shotchart_df_all = dataframes['shotchart_df_all']
playertracking_df_all = dataframes['playertracking_df_all']
advanced_box_df_all = dataframes['advanced_box_df_all']
play_by_play_df_all = dataframes['play_by_play_df_all']
traditional_box_df_all = dataframes['traditional_box_df_all']
team_advanced_df_all = dataframes['team_advanced_df_all']
team_game_logs_all = dataframes['team_game_logs_df_all']

for file_suffix, df in data_types.items():
    path = f"{finished_data_path}combined{file_suffix}"
    update_csv(path,dataframes[df]) 


#log update
with open(log_file, 'a') as f:
    f.write(f"Processed game IDs: {filtered_game_ids}\n")
    f.write(f"Total games processed: {len(filtered_game_ids)}\n")
    f.write(f"Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
