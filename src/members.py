import sys
from pathlib import Path

import pandas as pd
import numpy as np

from .utils.preprocessing import *

# read in the raw data as a pandas dataframe
df = read_dbf('./data/raw/members.DBF')

# create expedition PK column
df.expid = df.expid.str.cat(df.myear, sep='_')

# CLIMBERS DATAFRAME
# extract climber data
climber_cols = ['fname', 'lname', 'sex', 'yob', 'citizen', 'residence', 'occupation']
df_climbers = df[climber_cols].drop_duplicates(ignore_index=True)

# create unique id for climbers
df_climbers['climber_id'] = df_climbers.groupby(['fname', 'lname', 'sex', 'yob']).transform('ngroup')

# replace climber data with id
df = df.merge(df_climbers, how='left').drop(climber_cols, axis=1)

print(df.head())
