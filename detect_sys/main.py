## Standarrd library
from pathlib import Path                    # For filesystem path and operations.
import pandas as pd                         # For data analysis.
import numpy as np                          
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
    ensure_ascii=False : Don't convert non-ASCII characters to \\uXXXX.
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
'''
    Initialize the previous state for each signal.
'''
prev_vals = {}
for sig in signals:
    if sig in prev_last.index:
        try:
            prev_vals[sig] = int(prev_last[sig])
        except:
            prev_vals[sig] = 0
    else:
        prev_vals[sig] = 0

## Ouput buffer for each 'Name'
'''
    An output buffer that temporarily stores the completed X-Y event pairs for each 'Name'
'''
pairs_buf = defaultdict(list) #name -> list[(x_time, y_time, dur)]

### streaming process
usecols = ['TIME'] + signals 
dtype_map = {sig:'Int8' for sig in signals} #Create a dtype map to read all signal columns as Int8.

processd_ok = []
total_file = len(pending_files)

for i, (f_abs, f_rel) in enumerate(zip(pending_files, pending_rel), start=1):

    print (f'[{i}/{total_file}] Processing: {f_rel}')

    if WRITE_GUARD:
        if not stable(f_abs, WRITE_WAIT):
            continue

    '''
        usecols:Read only the specified columns.
        dtype:Set the data type for each (specified) column.
        low_memory:Control type inferenve strategy(memory usage vs. consistency).
    '''
    df = pd.read_csv(
        f_abs, header=HDRROW, encoding=ENC,
        usecols=[c for c in usecols if c],
        dtype=dtype_map, low_memory=False
    )
    if df.empty:
        processd_ok.append(f_rel)
        continue

    #Parse TIME just onece here, keep as Timestamp for later arithmetic.
    #df['TIME'] = pd.to_datetime(df['TIME'], errors='coerce')
    rising = {}

    for sig in signals:
        if sig not in df.columns: #If the CSV doesn't have this signal column, skip it for this file.
            rising[sig] = None
            continue

        s = df[sig].astype('Int8').fillna(0).astype('Int8') #Normalize to 0/1 abd Int8 for robust comparisons.
        #Prepared the previous file's last value to build 'previous-in-file' series.
        prev = [prev_vals[sig]]
        prev_in_file = pd.concat([pd.Series(prev),s[:-1]], ignore_index=True)

        '''
            [Exapmle]
             prev =         0
             s =            [0, 0, 0, 1, 1, 1, 0, 0, 0]
             prev_in_file = [0, 0, 0, 0, 1, 1, 1, 0, 0]
             mask         = [F, F, F, T, F, F, F, F, F]
        '''
        if DEBOUNCE_N > 1:
            stable_mask = s.rolling(DEBOUNCE_N, min_periods=DEBOUNCE_N).sum()==DEBOUNCE_N
            stable_mask = stable_mask.fillna(False)
            mask = (prev_in_file==0) & (s==1) & stable_mask
        else:
            mask = (prev_in_file==0) & (s==1)

        rising[sig] = mask.values
        #Save this file's last value for the next file's boundary condition.
        prev_vals[sig] = int(s.iloc[-1])
    
    '''
        Build (time, name, IO(X/Y)) events from rising edges.
    '''
    events = []
    ts = df['TIME'].values #Numpy array of Timetamps

    for sig, mask in rising.items():
        if mask is None:
            continue
        if mask.any(): 
            times = ts[mask] #Times where rising occurred for this signal.
            for t in times:
                #Expand to all Names mapped from this signal on X and Y sides.
                for name in x_map.get(sig, []): 
                    events.append((t, name, 'X'))
                for name in y_map.get(sig, []): 
                    events.append((t, name, 'Y'))
    
    if events: 
        io_order = {'X':0,'Y':1} #Sort by time, and for the same timestamp process X before Y.
        '''
            lambda return():Unnamed function 
            x[0]:First sort key
            io_order[x[2]]:Second sort key
        '''
        events.sort(key=lambda x:(x[0], io_order[x[2]]))

        '''
            IF io is X then, store as unmatched X(FIFO queue).
            If io is Y then, pair with the oldest X and compute furation.
            Apply thresholds if set.
        '''
        for t, name, io in events:
            if io=='X': 
                open_x[name].append(t)
            else: # io == 'Y'
                if open_x[name]:
                    x_time = open_x[name].pop(0)
                    dur_ms = int((pd.to_datetime(t)-pd.to_datetime(x_time)).total_seconds()*1000)
                    if DUR_MIN and dur_ms < DUR_MIN:
                        continue
                    if DUR_MAX and dur_ms > DUR_MAX:
                        continue
                    pairs_buf[name].append((tstr(x_time), tstr(t), dur_ms))
    '''
        Update the previous snapshot(for the next file's boundary condition).
    '''
    last_row = df.iloc[[-1]]
    for sig in signals:
        if sig not in last_row.columns: 
            last_row[sig] = prev_vals[sig]
    cols = ['TIME'] + signals
    last_row[cols].to_csv(PREV_PATH, encoding=ENC, index=False)

    processd_ok.append(f_rel)

## Write on CSV files each Name
for name, rows in pairs_buf.items():
    if not rows:
        continue
    fname = os.path.join(OUTPUT_DIR, f'{sanitize(name)}.csv')
    dfw = pd.DataFrame(rows, columns=['X_TIME', 'Y_TIME', 'DURATION_MS'])
    header = not os.path.exists(fname)  #Write header only when creating a new file.
    dfw.to_csv(fname, mode='a', index=False, header=header, encoding=ENC) 

## Write output.csv only ummatch pair X 
rem=[]
for name, times in open_x.items():
    for t in times:
        rem.append((name, tstr(t), 'X'))

if rem:
    rem_df = pd.DataFrame(rem, columns=['Name', 'TIME', 'IO'])
    rem_df['TIME'] = pd.to_datetime(rem_df['TIME'], errors='coerce')
    rem_df.sort_values(by=['Name', 'TIME', 'IO'], inplace=True)
    rem_df['TIME'] = rem_df['TIME'].apply(tstr)
    rem_df.to_csv(OUT_EVENTS, index=False, encoding=ENC)
else:
    if os.path.exists(OUT_EVENTS):
        os.remove(OUT_EVENTS)

## Update state.json
state['processed_paths'] = sorted(set(already).union(processd_ok))
save_state(STATE_PATH, state)
