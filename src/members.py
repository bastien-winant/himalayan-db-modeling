from .utils.preprocessing import *
from .utils.maps import *

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

# remove unused columns
df.drop([ 'peakid', 'myear', 'mseason', 'age', 'birthdate', 'calcage'], axis=1, inplace=True)

print(df.mroute1.value_counts())
print(df.mroute2.value_counts())
print(df.mroute3.value_counts())

fact_ascents_0 = df.loc[df.mroute1 == 0, :] \
	.drop(['mroute1', 'msmtdate1', 'msmttime1', 'mascent1', 'msmtnote1',
				 'mroute2', 'msmtdate2', 'msmttime2', 'mascent2', 'msmtnote2',
				 'mroute3', 'msmtdate3', 'msmttime3', 'mascent3', 'msmtnote3'], axis=1)
fact_ascents_0['ascent_number'] = 0

fact_ascents_1 = df.loc[df.mroute1 > 0, :] \
	.drop(['mroute1', 'msmtdate1', 'msmttime1', 'mascent1', 'msmtnote1',
				 'mroute2', 'msmtdate2', 'msmttime2', 'mascent2', 'msmtnote2',
				 'mroute3', 'msmtdate3', 'msmttime3', 'mascent3', 'msmtnote3'], axis=1)
fact_ascents_1['ascent_number'] = 1

fact_ascents_2 = df.loc[df.mroute2 > 0, :] \
	.drop(['mroute1', 'msmtdate1', 'msmttime1', 'mascent1', 'msmtnote1',
				 'mroute2', 'msmtdate2', 'msmttime2', 'mascent2', 'msmtnote2',
				 'mroute3', 'msmtdate3', 'msmttime3', 'mascent3', 'msmtnote3'], axis=1)
fact_ascents_2['ascent_number'] = 2

fact_ascents_3 = df.loc[df.mroute3 > 0, :] \
	.drop(['mroute1', 'msmtdate1', 'msmttime1', 'mascent1', 'msmtnote1',
				 'mroute2', 'msmtdate2', 'msmttime2', 'mascent2', 'msmtnote2',
				 'mroute3', 'msmtdate3', 'msmttime3', 'mascent3', 'msmtnote3'], axis=1)
fact_ascents_3['ascent_number'] = 3

df = pd.concat([fact_ascents_0, fact_ascents_1, fact_ascents_2, fact_ascents_3], ignore_index=True)