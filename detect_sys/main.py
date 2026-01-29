## Standarrd library
from pathlib import Path                    # For filesystem path and operations.
import pandas as pd                         # For data analysis.
import os                                   # For OS-dependent features.
import re                                   # For regular expression.
import json                                 # For working with JSON.
import tomllib                              # For working with TOML.
from datetime import datetime, timedelta    # For the current date/time and time differences.
from collections import defaultdict         # For automatically initializing missing dictionary keys.

## Read Setting(TOML)
'''
    # If the toml library is None or the path does nnot exist, return None.
    # Otherwise, load TOML file.
'''
def load_toml(path):
    if tomllib is None or not os.path.exists(path):
        return {}
    with open(path, 'rb') as f:
        return tomllib.load(f)
    
'''
    #IF cfg is not a dictonary or any key is missing, return default.
    #Otherwise, return the value for the given key path.
'''
def cfg(cfg, keys, default=None):
    cur = cfg
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

## Get Config
# workdirectory is based on script path
os.chdir(Path(__file__).resolve().parent)

CFG = load_toml('config.toml')

ENC         = cfg(CFG, ('io', 'encoding'), 'CP932')                             #Encoding
HDRROW      = cfg(CFG, ('io', 'header_row'), 2)                                 #Header_row_count

BASE_DIR    = cfg(CFG, ('paths', 'base_dir'))                                   #Original_Data_directory
CSV_GLOB    = cfg(CFG, ('paths', 'glob_csv'), '*.csv')                          #Target_files
PREV_PATH   = cfg(CFG, ('paths', 'prev_path'), 'table/d_tube_assembly.csv')     #Previous_Data_file
CROSS_XLSX  = cfg(CFG, ('paths', 'cross_xlsx'))                                 #Cross_Table_file
OUT_EVENTS  = cfg(CFG, ('paths', 'out_events'))                                 #Unmatch_file
OUTPUT_DIR  = cfg(CFG, ('paths', 'output_dir'))                                 #Ouput_directory
STATE_PATH  = cfg(CFG, ('paths', 'state_path'), 'state.json')                   #Already_processed_file

DEBOUNCE_N  = int(cfg(CFG, ('logic', 'debounce_n'), 1))                         #Switch_debouncing(1:OFF, 3>=:ON)
DUR_MIN     = int(cfg(CFG, ('logic', 'duration_min_ms'), 0))                    #Duration_minimum_seconds(0:OFF)
DUR_MAX     = int(cfg(CFG, ('logic', 'duration_max_ms'), 0))                    #Duration_maximum_seconds(0:OFF)
WRITE_GUARD = bool(cfg(CFG, ('logic', 'write_guard_enable'), False))            #Write_protection
WRITE_WAIT  = int(cfg(CFG, ('logic', 'write_guard_wait_ms'), 300))              #Write_protection_seconds
RECENT_DAYS = int(cfg(CFG, ('logic', 'recent_days'), 0))                        #Last_N_days(0:all)

## Make dirctory
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(Path(PREV_PATH).parent, exist_ok=True)

## utils
'''
    If the file exist and is not empty, load and return it as JSON.
    Otherwise, return the default state indicate that no files have been processed.
'''
def load_state(path):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except:
                pass
    return {"processed_paths":[]}

'''
    Save the state to a JSON file.
    ensure_ascii=False : Don't convert non-ASCII characters to \uXXXX.
'''
def save_state(path, state):
    with open(path, 'w', encoding='utf-8') as w:
        json.dump(state, w, ensure_ascii=False, indent=2)

'''
    Compare the sile size before and after waitng wait_ms.
    Return true if the file size doesn't change during wait_ms.
'''
def stable(path, wait_ms):
    try:
        s1 = os.path.getsize(path)
        import time; time.sleep(wait_ms/1000)
        s2 = os.path.getsize(path)
        return s1==s2
    except:
        return False

'''
    Convert a datetime to a string formatted as YYYY-MM-DD HH:MM:SS.fff.
    Return an empty string if the value is NaN/NaT.
'''
def tstr(t):
    if pd.isna(t):return ''
    return pd.to_datetime(t).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

'''
    Sanitize a filename.
    re.sub():Replace parts of a string that match a regular expression.
    .strip():Remove leading and trailing spaces. If the result is empty, use "noname" instead.
'''
def sanitize(s):
    return re.sub(r'[\\/:*?"<>|]', '_', str(s).strip() or 'noname')

## load prev snapshot
'''
    If the previous file exists and is not empty, read it and get the last row.
    Otherwise, prepare an empty dataset.
'''
if os.path.exists(PREV_PATH) and os.path.getsize(PREV_PATH) > 0:
    prev_df = pd.read_csv(PREV_PATH, encoding=ENC)
    prev_last = prev_df.iloc[-1]
else:
    prev_last = pd.Series(dtype='object')

## load cross table
'''
    Create from CROSS_XLSX a list of name associated with each x and eaxh y.
    Collect all x and y values that appear, removing duplicates.
'''
cross = pd.read_excel(CROSS_XLSX)
for col in ['name', 'x', 'y']:
    if col not in cross.columns:
        raise ValueError('CROSS_XLSX does not contain the columns [name], [x] and [y]')

x_map = defaultdict(list) #A dictionary that automatically initializes missing keys with an empty list.
y_map = defaultdict(list) 
signals = set() #A set is an unorderded collection of unique elements.set() creates an empty set.
for _, r in cross.iterrows():
    name = r['name']
    x = str(r['x'])
    y = str(r['y'])
    x_map[x].append(name) 
    y_map[y].append(name)
    signals.add(x)
    signals.add(y)
signals = sorted(signals)

'''
    Read to JSON file
'''
## state JSON
state = load_state(STATE_PATH)
already = set(state['processed_paths']) #Creates a set of unique file path strings.

## explored
base = Path(BASE_DIR)
if not base.exists():
    raise FileNotFoundError(f'Not exist base folder:{base}')

files_all = list(base.rglob(CSV_GLOB))

'''
    Convert file paths to paths rel(ative) to 'base'(POSIX-style strings).
    .as_posix():Convert \(back slash) to /(slash).
'''
files_rel = [f.relative_to(base).as_posix() for f in files_all]

# Select only within RECENT_DAYS(0:OFF)
if RECENT_DAYS > 0:
    '''
        This checks whether rel contains an 8-digit date and whether that date is within RECENT_DAYS.
        re.findall():Finds all string and returns them as a list.
        datetime.strptime():Parses a date/time string according to a specified format and returns a datetime object.
    '''
    def is_recent(rel):
        digits = re.findall(r'\d{8}',rel)
        if not digits:
            return False
        d = digits[0]
        try:
            dt = datetime.strptime(d, '%Y%m%d')
            return dt >= (datetime.now() - timedelta(days=RECENT_DAYS))
        except:
            return False
    files_rel = [r for r in files_rel if is_recent(r)]

# Select only unprocessed data
pending_rel = [r for r in files_rel if r not in already]
if not pending_rel:
    print("not New files, Exit.")
    raise SystemExit(0)

# Return the absolute path
pending_files = [str(base / r) for r in pending_rel] 
pending_files = sorted(pending_files, key=lambda p:os.path.getmtime(p))

## load open-X(unmatch state)
open_x  = defaultdict(list)
if os.path.exists(OUT_EVENTS) and os.path.getsize(OUT_EVENTS) > 0:
    ex = pd.read_csv(OUT_EVENTS, encoding=ENC)
    if not ex.empty:
        ex['TIME'] = pd.to_datetime(ex['TIME'], errors='coerce') # errors='coerce':Values that can't be parsed are set to NaT(treated as missing).
        for nm, g in ex.groupby('Name'): #Group by the 'Name'.nm is tha name, and g is the DataFrame for that name.
            open_x[nm] = list(g['TIME'].dropna().sort_values().values) #Drop missing times, sort by TIME, and convert to a python list.

## prev_vals

