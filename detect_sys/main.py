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
#[Function]Load to toml_files
'''
    # If the toml library is None or the path does nnot exist, return None.
    # Otherwise, load TOML file.
'''
def load_toml(path):
    if tomllib is None or not os.path.exists(path):
        return {}
    with open(path, 'rb') as f:
        return tomllib.load(f)
    
#[Function]Get Config 
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

## Get ConfigP
#workdirectory is based on script path
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

DEBOUNCE_N  = int(cfg(CFG, ('logic', 'debounce_n'), 1))                         #Switch_debouncing(1:OFF,3>=:ON)
DUR_MIN     = int(cfg(CFG, ('logic', 'duration_min_ms'), 0))                    #Duration_minimum_seconds(0:OFF)
DUR_MAX     = int(cfg(CFG, ('logic', 'duration_max_ms'), 0))                    #Duration_maximum_seconds(0:OFF)
WRITE_GUARD = bool(cfg(CFG, ('logic', 'write_guard_enable'), False))
