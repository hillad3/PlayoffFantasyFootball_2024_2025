## Data sourced from the nflverse github: 
## https://github.com/nflverse/nflverse-data

# %%
from requests import get
import os
import pandas as pd
import logging
from typing import List
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.WARNING, datefmt='%Y-%m-%d %I:%M %p')

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

# the teams data is mostly static for each year, so this does not need to be updated frequently
teams_url = 'https://github.com/nflverse/nfldata/blob/master/data/teams.csv?raw=true'
refresh_teams = False

# local filePath helper
data_path = './Data/'

# %%
def get_teams_csv(update_teams : bool = refresh_teams, url : str = teams_url) -> None:
  if not update_teams:
    logging.warning("nflreadr teams not updated")
    return
  else:
    req = get(url)
    if req.status_code == 200:
      open(data_path + 'nflreadr_teams.csv', 'wb').write(req.content)
      logging.warning("nflreadr teams updated")
    else:
      logging.warning(f"Failed to retrieve the timestamp.txt file. Status code: {req.status_code}")

get_teams_csv() # use default values

# %%
def get_nflverse_timestamp(url : str) -> str:
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
    date_time = req.text.replace("\n","").replace(":","")
    logging.info("nflverse asset timestamp: " + date_time + "\n")
    return date_time
  else:
    logging.warning(f"Failed to retrieve the timestamp.txt file. Status code: {req.status_code}")
    return '9999-99-99 999999 EST' # return dummy that will always result in the data file being updated

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
def parse_local_filename(file_name: str) -> str:
  """
  Parses timestamp from local file in data_path directory.
  """
  timestamp = file_name[file_name.rfind(',')+2:]
  timestamp = timestamp[:timestamp.rfind(r'.')]
  logging.info("Local timestamp: " + timestamp)
  return timestamp

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
def remove_old_file(file_name : str) -> None:
  """
  Delete the file name. Called from is_new_nflverse_file when True.
  """
  logging.warning('Removing local file: ' + file_name)
  os.remove(data_path + file_name)


# %%
def is_new_nflverse_file(url: str) -> bool:
  """
  The timestamp.txt github URL for the nflverse data file is fetched and compared against
  the timestamp in the local data file name. The local file is determine based on a substring 
  match parsed from the github URL being requested. If matched, delete the old file.
  """

  github_file_name = parse_nflverse_filename(url)[0]

  data_files = os.listdir(data_path)

  for d in data_files:
    logging.info("Local file: " + d)
    logging.info("Target substring: " + github_file_name)
    logging.info("Substring match test: " + str(github_file_name in d))
    if github_file_name in d:
      local_timestamp = parse_local_filename(d)
      github_timestamp = get_nflverse_timestamp(url)
      logging.info("Github file name substring matches an existing file. Evaluating timestamps...")
      if local_timestamp != github_timestamp:
        logging.info("Timestamps indicate a new data file.")
        remove_old_file(d)
        return True
      else:
        logging.info("Timestamps indicate no change to data file.")
        return False
         
  logging.info("No substring match found with files in data_path.")  
  return False

# %%
def update_nflverse_data(url : str, force_update : bool = False) -> None:
  """
  The URL for the data file is evalauted to determine which Github
  file is to be downloaded, if any (based on timestamp comparison to local 
  file). Currently using *.parquet files to keep requests and data storage 
  smaller but in theory this function is agnostic to file type (.csv 
  and .rds are available)
  """

  perform_update = is_new_nflverse_file(url)

  if perform_update:
      new_data_file_name = make_new_file_name(url)
      logging.warning('Writing new local file: ' + new_data_file_name + '\n')
      df = pd.read_parquet(url, engine = 'pyarrow')
      df.to_parquet(data_path + new_data_file_name, engine='pyarrow')
  elif force_update:
      new_data_file_name_forced = make_new_file_name(url)
      logging.warning('Writing new local file (forced): ' + new_data_file_name + '\n')
      df = pd.read_parquet(url, engine = 'pyarrow')
      df.to_parquet(data_path + new_data_file_name_forced, engine='pyarrow')
      logging.warning('No file cleanup performed. Any old data files should be deleted manually.')  
  elif not perform_update and not force_update:
      logging.warning('Local file is up-to-date. No action performed.')
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
