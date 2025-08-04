from dbfread import DBF
import pandas as pd
import csv

def read_dbf(dfb_file_path: str) -> pd.DataFrame:
	dbf = DBF(dfb_file_path)
	df = pd.DataFrame(iter(dbf))

	df.replace('', None, inplace=True)
	df.where(df.notna(), None, inplace=True)

	df.columns = [col.lower() for col in df.columns]

	return df

def float_to_int(s: pd.Series) -> pd.Series:
	s = s.fillna(-999).astype(int).replace(-999, None)

	return s

def apply_map(s: pd.Series, map: dict) -> pd.Series:
	return s.apply(lambda x: map.get(x))

def update_country_list(df, country_col):
	df = df.copy()
	df[country_col] = df[country_col].str.lower()

	temp_df = df[[country_col]].drop_duplicates()
	temp_df['country_list_name'] = temp_df[country_col]
	temp_df.drop(country_col, axis=1, inplace=True)

	country_df = pd.DataFrame(columns=['country_list_name'])
	try:
		country_df = pd.read_csv('./data/processed/countries.csv')
	except FileNotFoundError:
		pass

	country_df = pd.concat([country_df, temp_df])\
		.drop_duplicates(ignore_index=True)\
		.reset_index(names='country_list_id')

	df = df\
		.merge(country_df, how="left", left_on=country_col, right_on='country_list_name')\
		.rename({'country_list_id': f"{country_col}_id"})\
		.drop([country_col, 'country_list_name'], axis=1)

	country_df.drop('country_list_id', axis=1).to_csv('./data/processed/countries.csv', index=False)

	return df

def write_csv(df: pd.DataFrame, csv_file_path: str) -> None:
	df.to_csv(csv_file_path, index=False)