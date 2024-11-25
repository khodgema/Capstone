import pandas as pd 
import os
from datetime import datetime
import time 

log_file = f"combine_data.txt"
with open(log_file, 'a') as f:
    f.write(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


# Combine data from all seasons in the current folder
season_files = [f for f in os.listdir('.') if f.endswith('_shotchart_data.csv') or
                                                f.endswith('_playertracking_data.csv') or
                                                f.endswith('_advanced_box_data.csv') or
                                                f.endswith('_play_by_play.csv') or
                                                f.endswith('_box_data.csv')]
shotchart_df_all = pd.DataFrame()
playertracking_df_all = pd.DataFrame()
advanced_box_df_all = pd.DataFrame()
play_by_play_df_all = pd.DataFrame()
traditional_box_df_all = pd.DataFrame()

for season_file in season_files:
    shotchart_df = pd.read_csv(season_file)
    playertracking_df = pd.read_csv(season_file)
    advanced_box_df = pd.read_csv(season_file)
    play_by_play_df = pd.read_csv(season_file)
    traditional_box_df = pd.read_csv(season_file)

    # Combine dataframes
    shotchart_df_all = pd.concat([shotchart_df_all, shotchart_df], ignore_index=True)
    playertracking_df_all = pd.concat([playertracking_df_all, playertracking_df], ignore_index=True)
    advanced_box_df_all = pd.concat([advanced_box_df_all, advanced_box_df], ignore_index=True)
    play_by_play_df_all = pd.concat([play_by_play_df_all, play_by_play_df], ignore_index=True)

# Save combined dataframes to files
shotchart_file_all = 'shotchart_data_all.csv'
playertracking_file_all = 'playertracking_data_all.csv'
advanced_box_file_all = 'advanced_box_data_all.csv'
play_by_play_file_all = 'play_by_play_all.csv'
traditional_box_file_all = 'box_data_all.csv'

# Function to append or create a CSV file
def update_csv(file_path, df):
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined_df = df

    combined_df.to_csv(file_path, index=False)

update_csv(shotchart_file_all, shotchart_df_all)
update_csv(playertracking_file_all, playertracking_df_all)
update_csv(advanced_box_file_all, advanced_box_df_all)
update_csv(play_by_play_file_all, play_by_play_df_all)
update_csv(traditional_box_file_all, traditional_box_df_all)

# Log update
with open('log.txt', 'a') as f:
    f.write(f"Combined data for all seasons\n")




