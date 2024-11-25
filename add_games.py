
from nba_api.stats.endpoints import ShotChartDetail, LeagueGameLog, BoxScorePlayerTrackV3, BoxScoreAdvancedV3, playbyplayv3, SynergyPlayTypes, BoxScoreTraditionalV3, playbyplay
import pandas as pd
import time
from datetime import datetime

# This function retrieves all game IDs for a given season and season type.
def get_game_ids(season, season_type='Regular Season'):
  # Get the game log data for the given season and season type.
    game_log = LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
    # Extract unique game IDs from the game log data.
    game_ids = game_log['GAME_ID'].unique().tolist()
    return game_ids

# This function retrieves the current NBA season.
def get_current_season():
    # Get the current date and time.
    current_date = datetime.now()
    # Extract the year from the current date.
    year = current_date.year
    # Determine whether the current month is before or after October to decide the season.
    if current_date.month < 10:
        season = str(year - 1) + '-' + str(year)[2:]
    else:
        season = str(year) + '-' + str(year + 1)[2:]
    return season

# This function retrieves data for all games in a given list of game IDs.
def get_data(game_ids):
    # Initialize empty lists to store data for each type of statistic.
    play_by_play_data = []
    shotchart_data  = []
    playertracking_data  = []
    advanced_data  = []
    box_data  = []

    # Loop through each game ID in the given list of game IDs.
    for game_id in game_ids:
        retry_count = 0
        max_retries = 3

        # Try to retrieve data for the current game ID up to a maximum number of retries.
        while retry_count < max_retries:
            try:
                # Rate limiting - wait for 1 second before making another request.
                time.sleep(2)

                # Retrieve standard box score data for the current game ID.
                try:
                    box  = BoxScoreTraditionalV3(game_id  = game_id).get_data_frames()[0]
                    box['GAME_ID']  = game_id
                    box_data.append(box)
                except Exception as e:
                    print(f"Failed box traditional for {game_id}: {e}")
                    time.sleep(5)

                # Retrieve play-by-play data for the current game ID.
                try:
                    play_by_play = playbyplay.PlayByPlay(game_id=game_id).get_data_frames()[0]
                    play_by_play['GAME_ID']  = game_id
                    play_by_play_data.append(play_by_play)
                except Exception as e:
                    print(f"Failed PlayByPlayV3 for {game_id}: {e}")
                    time.sleep(5)

                # Retrieve shot chart data for the current game ID.
                try:
                    shotchart = ShotChartDetail(game_id_nullable=game_id, team_id=0, player_id=0,context_measure_simple='FGA').get_data_frames()[0]
                    shotchart['GAME_ID']  = game_id
                    shotchart_data.append(shotchart)
                except Exception as e:
                    print(f"Failed ShotChartDetail for {game_id}: {e}")
                    time.sleep(5)

                # Retrieve player tracking data for the current game ID.
                try:
                    player_tracking  = BoxScorePlayerTrackV3(game_id=game_id).get_data_frames()[0]
                    player_tracking['GAME_ID']  = game_id
                    playertracking_data.append(player_tracking)
                except Exception as e:
                    print(f"Failed BoxScorePlayerTrackV3 for {game_id}: {e}")
                    time.sleep(5)

                # Retrieve advanced box score data for the current game ID.
                try:
                    advanced_boxscore  = BoxScoreAdvancedV3(game_id=game_id).get_data_frames()[0]
                    advanced_boxscore['GAME_ID']  = game_id
                    advanced_data.append(advanced_boxscore)
                except Exception as e:
                    print(f"Failed BoxScoreAdvancedV3 for {game_id}: {e}")
                    time.sleep(5)

                    break # exit the while loop if all requests succeed
            except Exception as e:
                print(f"Unexpected error for game {game_id}: {e}")
                retry_count += 1
                time.sleep(5)


    shotchart_df  = pd.concat(shotchart_data, ignore_index=True) if shotchart_data else pd.DataFrame()
    playertracking_df  = pd.concat(playertracking_data, ignore_index=True) if playertracking_data else pd.DataFrame()
    advanced_boxscore_df  = pd.concat(advanced_data, ignore_index=True) if advanced_data else pd.DataFrame()
    playbyplayv3_df  = pd.concat(play_by_play_data,ignore_index=True) if play_by_play_data else pd.DataFrame()
    traditional_box_df = pd.concat(box_data,ignore_index=True) if box_data else pd.DataFrame()

    return shotchart_df, playertracking_df ,advanced_boxscore_df,playbyplayv3_df, traditional_box_df


# Retrieve the current NBA season.
season =  get_current_season()

# Get a list of game IDs for the current season.
game_ids  = get_game_ids(season = season)

# Check if there are any games to process.
if 1 < 11:
    # Retrieve data for all games in the current season.
    shotchart_df,playertracking_df, advanced_boxscore_df, playbyplayv3_df, traditional_box_df=get_data(game_ids)

    # Create file names for each type of statistic based on the current season.
    shotchartfile = f"{season}_shotchart.csv"
    playertrackingfile  = f"{season}_playertracking.csv"
    advancedboxscorefile = f'{season}_advancedboxscore.csv'
    playbyplayv3file = f"{season}_playbyplayv3.csv"
    traditional_boxfile = f"{season}_box_data.csv"

    # Save each type of statistic data to a CSV file.
    shotchart_df.to_csv(shotchartfile,index=False)
    playertracking_df.to_csv(playertrackingfile, index=False)
    advanced_boxscore_df.to_csv(advancedboxscorefile ,index=False)
    playbyplayv3_df.to_csv (playbyplayv3file, index=False)
    traditional_box_df.to_csv(traditional_boxfile, index=False)


# Print the list of game IDs for the current season.
print(get_game_ids(get_current_season()))
