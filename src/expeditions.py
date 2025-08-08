from unittest.mock import inplace

from .utils.preprocessing import *
from .utils.maps import *

# read in the raw data as a pandas dataframe
dim_exped = read_dbf('./data/raw/exped.DBF')

dim_exped['expedition_id'] = dim_exped.expid.str.cat(dim_exped.year, sep='_')

# SUMMIT/TERMINATION DATES
dim_exped.bcdate = pd.to_datetime(dim_exped.bcdate)
dim_exped.smtdate = pd.to_datetime(dim_exped.smtdate)
dim_exped.termdate = pd.to_datetime(dim_exped.termdate)

dim_exped.smtdays = dim_exped.smtdays.apply(lambda x: pd.Timedelta(days=x))
dim_exped.totdays = dim_exped.totdays.apply(lambda x: pd.Timedelta(days=x))

dim_exped.smtdate = dim_exped.smtdate.where(dim_exped.smtdate.notna(), dim_exped.bcdate + dim_exped.smtdays)
dim_exped.termdate = dim_exped.termdate.where(dim_exped.termdate.notna(), dim_exped.bcdate + dim_exped.totdays)

dim_exped.drop(
	['smtdays', 'totdays', 'totmembers', 'smtmembers', 'mdeaths', 'tothired', 'smthired', 'hdeaths', 'nohired'],
	axis=1, inplace=True)

dim_exped['termination_reason'] = apply_map(dim_exped.termreason, exped_termination_map)
dim_exped.drop('termreason', axis=1, inplace=True)

dim_exped.host = apply_map(dim_exped.host, exped_host_map)

# ROUTES
dim_route_1 = dim_exped.loc[
	dim_exped.route1.notna() | dim_exped.success1.notna() | dim_exped.ascent1.notna(),
	['expedition_id', 'route1', 'success1', 'ascent1']]\
	.drop_duplicates()\
	.rename({'route1': 'description', 'success1': 'success', 'ascent1': 'ascent_numbers'}, axis=1)
dim_route_1['number'] = 1

dim_route_2 = dim_exped.loc[
	dim_exped.route2.notna() | dim_exped.success2.notna() | dim_exped.ascent2.notna(),
	['expedition_id', 'route2', 'success2', 'ascent2']]\
	.drop_duplicates()\
	.rename({'route2': 'description', 'success2': 'success', 'ascent2': 'ascent_numbers'}, axis=1)
dim_route_2['number'] = 2

dim_route_3 = dim_exped.loc[
	dim_exped.route3.notna() | dim_exped.success3.notna() | dim_exped.ascent3.notna(),
	['expedition_id', 'route3', 'success3', 'ascent3']]\
	.drop_duplicates()\
	.rename({'route3': 'description', 'success3': 'success', 'ascent3': 'ascent_numbers'}, axis=1)
dim_route_3['number'] = 3

dim_route_4 = dim_exped.loc[
	dim_exped.route4.notna() | dim_exped.success4.notna() | dim_exped.ascent4.notna(),
	['expedition_id', 'route4', 'success4', 'ascent4']]\
	.drop_duplicates()\
	.rename({'route4': 'description', 'success4': 'success', 'ascent4': 'ascent_numbers'}, axis=1)
dim_route_4['number'] = 4

dim_route = pd.concat([dim_route_1, dim_route_2, dim_route_3, dim_route_4], ignore_index=True)

dim_exped.drop(
	['route1', 'success1', 'ascent1', 'route2', 'success2', 'ascent2', 'route3', 'success3', 'ascent3',
	 'route4', 'success4', 'ascent4'], axis=1, inplace=True)