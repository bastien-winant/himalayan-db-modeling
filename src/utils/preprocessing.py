from dbfread import DBF
import pandas as pd
import csv

def read_dbf(dfb_file_path):
	dbf = DBF(dfb_file_path)
	df = pd.DataFrame(iter(dbf))

	df.replace('', None, inplace=True)
	df.where(df.notna(), None, inplace=True)

	df.columns = [col.lower() for col in df.columns]

	return df

def write_csv(df, csv_file_path):
	df.to_csv(csv_file_path, index=False)