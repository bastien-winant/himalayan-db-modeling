from .utils.preprocessing import *
from .utils.maps import *

# read in the raw data as a pandas dataframe
dim_peak = read_dbf('./data/raw/peaks.DBF')

dim_peak.drop(
	['pyear', 'pseason', 'pexpid', 'psmtdate', 'pcountry', 'psummiters', 'psmtnote', 'heightf'],
	axis=1, inplace=True, errors='ignore')

# verify that peakid is a valid primary key
assert dim_peak.peakid.nunique() == dim_peak.shape[0]
assert dim_peak.peakid.isna().sum() == 0

# atomise peak local names
dim_local_names = dim_peak.loc[dim_peak.pkname2.notna(), ['peakid', 'pkname2']]
dim_local_names['local_name'] = dim_local_names\
	.pkname2.str.split(',')
dim_local_names = dim_local_names.drop('pkname2', axis=1).explode('local_name')
dim_local_names.local_name = dim_local_names.local_name.str.strip()
dim_peak.drop(['pkname2'], axis=1, inplace=True)


dim_peak['himal'] = apply_map(dim_peak.himal, himal_map)
dim_peak['region'] = apply_map(dim_peak.region, region_map)