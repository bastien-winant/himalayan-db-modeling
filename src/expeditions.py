import sys
from pathlib import Path

import pandas as pd
import numpy as np

from .utils.preprocessing import *

# read in the raw data as a pandas dataframe
df = read_dbf('./data/raw/exped.DBF')

# create expedition PK column
df['id'] = df.expid.str.cat(df.year, sep='_')

# ROUTES
df_ascent_1 = df.loc[
	df.route1.notna(),
	['id', 'route1', 'success1', 'ascent1']
].rename({
	'route1': 'route_description',
	'success1': 'success',
	'ascent1': 'ascent'
}, axis=1)
df_ascent_1['number'] = 1

df_ascent_2 = df.loc[
	df.route2.notna(),
	['id', 'route2', 'success2', 'ascent2']
].rename({
	'route2': 'route_description',
	'success2': 'success',
	'ascent2': 'ascent'
}, axis=1)
df_ascent_2['number'] = 2

df_ascent_3 = df.loc[
	df.route3.notna(),
	['id', 'route3', 'success3', 'ascent3']
].rename({
	'route3': 'route_description',
	'success3': 'success',
	'ascent3': 'ascent'
}, axis=1)
df_ascent_3['number'] = 3

df_ascent_4 = df.loc[
	df.route4.notna(),
	['id', 'route4', 'success4', 'ascent4']
].rename({
	'route4': 'route_description',
	'success4': 'success',
	'ascent4': 'ascent'
}, axis=1)
df_ascent_4['number'] = 4

df_ascents = pd.concat([df_ascent_1, df_ascent_2, df_ascent_3, df_ascent_4], ignore_index=True)

df.drop(
	['route1', 'success1', 'ascent1', 'route2', 'success2', 'ascent2',
	 'route3', 'success3', 'ascent3', 'route4', 'success4', 'ascent4'],
	axis=1, inplace=True
)

host_map = {
	0: 'unknown',
	1: 'Nepal',
	2: 'China',
	3: 'India'
}

df.loc[:, 'host_country'] = apply_map(df.host, host_map)
df.drop('host', axis=1, inplace=True)
df = update_country_list(df, 'host_country')

df.nation = df.nation.str.split('/')
df = df.explode('nation')
df = update_country_list(df, 'nation')

df.countries = df.countries.str.split(',')
df = df.explode('countries')
df.countries = df.countries.str.split('/')
df = df.explode('countries')
df = update_country_list(df, 'countries')

df.drop(
	['leaders', 'totmembers', 'smtmembers', 'mdeaths', 'tothired', 'smthired', 'hdeaths', 'nohired',
	 'o2used', 'o2none', 'o2climb', 'o2descent', 'o2sleep', 'o2medical', 'o2taken', 'o2unkwn'],
	axis=1, inplace=True)

# SUMMIT/TERMINATION DATES
df.bcdate = pd.to_datetime(df.bcdate)
df.smtdate = pd.to_datetime(df.smtdate)
df.termdate = pd.to_datetime(df.termdate)

df.smtdays = df.smtdays.apply(lambda x: pd.Timedelta(days=x))
df.totdays = df.totdays.apply(lambda x: pd.Timedelta(days=x))

df.smtdate = df.smtdate.where(df.smtdate.notna(), df.bcdate + df.smtdays)
df.termdate = df.termdate.where(df.termdate.notna(), df.bcdate + df.totdays)

df.drop(['smtdays', 'totdays'], axis=1, inplace=True)

print(df.columns)