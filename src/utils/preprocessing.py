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

def write_csv(df: pd.DataFrame, csv_file_path: str) -> None:
	df.to_csv(csv_file_path, index=False)