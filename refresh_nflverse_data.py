## Data sourced from the nflverse github: 
## https://github.com/nflverse/nflverse-data

# %%
from requests import get
import os
import pandas as pd
import logging
from typing import List
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %I:%M %p')

# %%
# https://github.com/nflverse/nflverse-data/releases/tag/rosters
team_rosters_timestamp_url = 'https://github.com/nflverse/nflverse-data/releases/download/rosters/timestamp.txt?raw=true'
team_rosters_url = 'https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_2024.parquet?raw=true'
team_rosters_urls = [team_rosters_timestamp_url, team_rosters_url]

# https://github.com/nflverse/nflverse-data/releases/tag/pbp
play_by_play_timestamp_url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/timestamp.txt?raw=true'
play_by_play_url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2024.parquet?raw=true'
play_by_play_urls = [play_by_play_timestamp_url, play_by_play_url]

# https://github.com/nflverse/nflverse-data/releases/tag/player_stats
player_stats_timestamp_url = 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/timestamp.txt?raw=true'
player_stats_off_url = 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_2024.parquet?raw=true'
player_stats_def_url = 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_def_2024.parquet?raw=true'
player_stats_kicking_url = 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_kicking_2024.parquet?raw=true'
player_stats_urls = [
  player_stats_timestamp_url, 
  player_stats_off_url, 
  player_stats_def_url, 
  player_stats_kicking_url
]


# %%
def pull_github_timestamp(url: str, fileName: str) -> None:
  req = get(url)
  file = open(fileName, 'wb')
  file.write(req.content)
  file.close()

# %%
def is_new_timestamp(url: str) -> bool:

  if 'rosters' in url:
    f1 = 'timestamp_team_rosters_tmp.txt'
    f2 = 'timestamp_team_rosters.txt'
  elif 'pbp' in url:
    f1 = 'timestamp_play_by_play_tmp.txt'
    f2 = 'timestamp_play_by_play.txt'
  elif 'player_stats' in url:
    f1 = 'timestamp_player_stats_tmp.txt'
    f2 = 'timestamp_player_stats.txt'
  else:
    ValueError('The keyword was not matched in the URL')

  logging.info('Temporary timestamp file created: ' + f1)
  logging.info('Existing timestamp file used for comparison: ' + f2)

  pull_github_timestamp(url, 'Data/' + f1)
  local_timestamp = open('Data/' + f2).read()
  github_timestamp = open('Data/' + f1).read()

  if github_timestamp==local_timestamp:
    logging.info('No changes to Github files. Removing temporary timestamp file: ' + f1)
    os.remove('Data/' + f1)
    return False
  else:
    # delete the old file and rename temp file; then refresh data
    logging.info('Github files have been updated. Replacing timestamp file with latest: ' + f2)
    os.remove('Data/' + f2)
    os.rename('Data/' + f1, 'Data/' + f2)
    return True


# %%
def update_nfl_data(urls : List[str], force_update : bool = False) -> None:

  timestamp_url = urls.pop(0)

  if 'rosters' in timestamp_url:
    fileName = ['team_roster_2024.parquet']
  elif 'pbp' in timestamp_url:
    fileName = ['play_by_play_2024.parquet']
  elif 'player_stats' in timestamp_url:
    fileName = ['player_stats_off_2024.parquet',
                'player_stats_def_2024.parquet',
                'player_stats_kicking_2024.parquet']
  else:
    ValueError('The keyword was not matched in the URL')

  logging.info(urls)
  logging.info(fileName)

  if is_new_timestamp(timestamp_url) or force_update:
    for i in range(0,len(urls)):
      logging.warning('UPDATING local file: ' + fileName[i] + '\n')
      df = pd.read_parquet(urls[i], engine = 'pyarrow')
      df.to_parquet('Data/' + fileName[i], engine='pyarrow')
  else:
    for i in range(0,len(urls)):
      logging.info(i)
      logging.info(urls[i])
      logging.info(fileName[i])
      logging.warning('NO UPDATE REQUIRED for ' + fileName[i] + '\n')


# %%
update_nfl_data(team_rosters_urls.copy())
update_nfl_data(play_by_play_urls.copy())
update_nfl_data(player_stats_urls.copy())
