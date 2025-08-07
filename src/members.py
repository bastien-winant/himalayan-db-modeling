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
climber_cols = ['fname', 'lname', 'sex', 'yob', 'citizen', 'residence', 'occupation', 'hcn']
dim_climber_df = df[climber_cols].drop_duplicates(ignore_index=True)

# create unique id for climbers
dim_climber_df['climber_id'] = dim_climber_df.groupby(['fname', 'lname', 'sex', 'yob']).transform('ngroup')

# replace climber data with id
df = df.merge(dim_climber_df, how='left').drop(climber_cols, axis=1)

df.drop([ 'peakid', 'myear', 'mseason', 'age', 'birthdate', 'calcage'], axis=1, inplace=True)

df_ascent_0 = df.loc[df.mroute1 == 0, :] \
	.drop(['mroute1', 'msmtdate1', 'msmttime1', 'mascent1', 'msmtnote1',
				 'mroute2', 'msmtdate2', 'msmttime2', 'mascent2', 'msmtnote2',
				 'mroute3', 'msmtdate3', 'msmttime3', 'mascent3', 'msmtnote3'], axis=1)
df_ascent_0['ascent_number'] = 0

df_ascent_1 = df.loc[df.mroute1 > 0, :] \
	.drop(['mroute2', 'msmtdate2', 'msmttime2', 'mascent2', 'msmtnote2',
				 'mroute3', 'msmtdate3', 'msmttime3', 'mascent3', 'msmtnote3'], axis=1) \
	.rename({'mroute1': 'route', 'msmtdate1': 'summit_date', 'msmttime1': 'summit_time', 'mascent1': 'ascent',
					 'msmtnote1': 'summit_note'}, axis=1)
df_ascent_1['ascent_number'] = 1

df_ascent_2 = df.loc[df.mroute2 > 0, :] \
	.drop(['mroute1', 'msmtdate1', 'msmttime1', 'mascent1', 'msmtnote1',
				 'mroute3', 'msmtdate3', 'msmttime3', 'mascent3', 'msmtnote3'], axis=1) \
	.rename({'mroute2': 'route', 'msmtdate2': 'summit_date', 'msmttime2': 'summit_time', 'mascent2': 'ascent',
					 'msmtnote2': 'summit_note'}, axis=1)
df_ascent_2['ascent_number'] = 2

df_ascent_3 = df.loc[df.mroute3 > 0, :] \
	.drop(['mroute1', 'msmtdate1', 'msmttime1', 'mascent1', 'msmtnote1',
				 'mroute2', 'msmtdate2', 'msmttime2', 'mascent2', 'msmtnote2'], axis=1) \
	.rename({'mroute3': 'route', 'msmtdate3': 'summit_date', 'msmttime3': 'summit_time', 'mascent3': 'ascent',
					 'msmtnote3': 'summit_note'}, axis=1)
df_ascent_3['ascent_number'] = 3

print(df.shape)
df = pd.concat([df_ascent_0, df_ascent_1, df_ascent_2, df_ascent_3], ignore_index=True)
print(df.shape)