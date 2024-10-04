## Data sourced from the nflverse github: 
## https://github.com/nflverse/nflverse-data

# %%
from requests import get
import os
import pandas as pd
import logging
from typing import List
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %I:%M %p')

## create lists of repo timestamps and raw urls
# %%
# https://github.com/nflverse/nflverse-data/releases/tag/rosters
team_rosters_url = 'https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_2024.parquet?raw=true'

# https://github.com/nflverse/nflverse-data/releases/tag/pbp
play_by_play_url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2024.parquet?raw=true'

# the player stats are split into three groups but share the same timestamp URL. 
# https://github.com/nflverse/nflverse-data/releases/tag/player_stats
player_stats_off_url = 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_2024.parquet?raw=true'
player_stats_def_url = 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_def_2024.parquet?raw=true'
player_stats_kicking_url = 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_kicking_2024.parquet?raw=true'

# local filePath helper
data_path = './Data/'

# %%
def get_nflverse_timestamp(url: str) -> str:
  """
  Each nflverse data repository has a timestamp key in a 
  timestamp.txt file. This function modifies the passed in 
  data file URL and re-points it to the timestamp.txt file.
  The get() request status is checked and if successful,
  returns the text so it can be evaluated against another 
  timestamp.   
  """
  url_path = url[:url.rfind('/')+1]
  logging.info("nflverse asset repository: " + url_path)
  timestamp_url = url_path + 'timestamp.txt?raw=true'
  req = get(timestamp_url)
  if req.status_code == 200:
    date_time = req.text.replace("\n","")
    logging.info("nflverse asset timestamp: " + date_time + "\n")
    return date_time
  else:
    print(f"Failed to retrieve the timestamp.txt file. Status code: {req.status_code}")
    return '9999-99-99 99:99:99 EST' # return dummy that will always result in the data file being updated

# %%
def parse_nflverse_filename(url: str) -> str:
  """
  Parses nflverse github data file URL. Element 0 is the file name handle. 
  Element 1 is the file type
  """
  github_file_string = url[url.rfind('/')+1:]
  github_file_string = github_file_string.replace(r"?raw=true","")
  github_file_string = github_file_string.split(r'.') 
  return github_file_string[0], github_file_string[1]

# %%
def make_new_file_name(url : str) -> str:
  """
  Construct the new file name based on file handle, timestamp, and file type.
  """
  github_timestamp = get_nflverse_timestamp(url)
  github_file_name, github_file_type = parse_nflverse_filename(url)
  logging.info("File Handle: " + github_file_name)
  logging.info("File Type: " + github_file_type)
  new_data_file_name = github_file_name + ", " + github_timestamp.replace(":","") + "." + github_file_type
  return new_data_file_name

# %%
def is_new_nflverse_file(url: str) -> bool:
  """
  The timestamp.txt github URL for the nflverse data file is fetched and compared against
  the timestamp in the local data file name. The local file is determine based on a substring 
  match parsed from the github URL being requested. This function returns multiple items: 
  First item is boolean to determine course of action in update_nflverse_data function. Second item
  is the local file name that will be deleted if there is a new timestamp.txt file. Third item
  is the new file name that will be written. If this function returns false, dummy parameters are 
  returned.
  """
  github_timestamp = get_nflverse_timestamp(url)
  
  github_file_name = parse_nflverse_filename(url)[0]

  data_files = os.listdir(data_path)
  logging.info("##### Directory Files #####")
  for i in data_files:
    logging.info(i)


  logging.info("\n")
  for d in data_files:
    logging.info("Local file: " + d)
    logging.info("Target substring: " + github_file_name)
    logging.info("Substring test: " + str(github_file_name in d))
    if github_file_name in d:
      local_file_to_delete = d
      logging.info("Match found. Returning actionable parameters.")
      return True, local_file_to_delete, make_new_file_name(url)

  logging.info("No match found. Returning dummy parameters.")  
  return False, "dummy.parquet", "dummy.parquet"

# %%
def update_nflverse_data(url : str, force_update : bool = False) -> None:
  """
  The URL for the data file is evalauted to determine which local
  file is to be updated if the timestamp.txt file indicates a different 
  timestamp in the local file name. If updating, delete the old data file
  and write the new data file. Currently using *.parquet files to keep
  requests and data storage smaller but in theory this function is 
  agnostic to file type (.csv and .rds are available)
  """

  perform_update, local_file_to_remove, new_data_file_name = is_new_nflverse_file(url)

  if perform_update:
      logging.warning('Removing local file: ' + local_file_to_remove + '\n')
      os.remove(data_path + local_file_to_remove)
      logging.warning('Writing new local file: ' + new_data_file_name + '\n')
      df = pd.read_parquet(url, engine = 'pyarrow')
      df.to_parquet(data_path + new_data_file_name, engine='pyarrow')
  elif force_update:
      github_file_name, github_file_type = parse_nflverse_filename(url)
      github_timestamp = get_nflverse_timestamp(url)
      new_data_file_name_forced = github_file_name + ", " + github_timestamp.replace(":","") + "." + github_file_type
      logging.warning('Writing new local file (forced): ' + new_data_file_name + '\n')
      df = pd.read_parquet(url, engine = 'pyarrow')
      df.to_parquet(data_path + new_data_file_name_forced, engine='pyarrow')
      logging.warning('No file cleanup performed. Any old data files should be deleted.')  
  elif not perform_update and not force_update:
      logging.warning('Local file is up-to-date. No action performedd.')
  else:
      logging.warning('No case matched to determine if update is needed.')


# %%
update_nflverse_data(team_rosters_url)

# %%
update_nflverse_data(play_by_play_url)

# %%
update_nflverse_data(player_stats_off_url)

# %%
update_nflverse_data(player_stats_def_url)

# %%
update_nflverse_data(player_stats_kicking_url)
