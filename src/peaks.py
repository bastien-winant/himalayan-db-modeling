from .utils.preprocessing import *
from .utils.maps import *
pd.options.mode.chained_assignment = None

# read in the raw data as a pandas dataframe
dim_peak = read_dbf('./data/raw/peaks.DBF')

# remove unused columns
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

# apply mapping as per documentation
dim_peak['himal'] = apply_map(dim_peak.himal, himal_map)
dim_peak['region'] = apply_map(dim_peak.region, region_map)

# normalised peak-country table
dim_peak_country = dim_peak[['peakid', 'phost']]
# apply the mapping as per the documentation and remove id columns
dim_peak_country['host'] = apply_map(dim_peak_country.phost, host_map)
dim_peak_country.drop('phost', axis=1, inplace=True)
dim_peak.drop('phost', axis=1, inplace=True)
# atomise the semicolumn-separated country names
dim_peak_country.host = dim_peak_country.host.str.split(';')
dim_peak_country = dim_peak_country.explode('host')
dim_peak_country = update_country_list(dim_peak_country, 'host')

print(dim_peak_country.head())