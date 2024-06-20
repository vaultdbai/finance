import requests
import os
import pandas as pd
import duckdb

def download(url, filename=None):
  """Downloads a file from a URL and saves it locally.

  Args:
    url: The URL of the file to download.
    filename: The filename to save the downloaded file as. If not specified,
      the filename will be extracted from the URL.
  """
  response = requests.get(url, stream=True)
  response.raise_for_status()
  
  if filename is None:
    filename = url.split("/")[-1]

  if os.path.isfile(filename):
    os.remove(filename)  

  if os.path.isfile(filename+".wal"):
    os.remove(filename+".wal")  

  with open(filename, "wb") as f:
    for chunk in response.iter_content(1024):
      if chunk:  # filter out keep-alive new chunks
        f.write(chunk)
        
  for x in os.listdir():
    if x.endswith(".db") or x.endswith(".wal"):
        # Prints only text file present in My Folder
        print(x)
        
  return filename

def sync_and_load(connection: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name:str, primary_keys:list[str]):
    df = df.infer_objects()
    try:
        connection.sql("CREATE OR REPLACE TABLE temp_sql AS SELECT * FROM df")
        df_tmp_sql = connection.sql("SHOW temp_sql;").fetchdf()    
        tbl_df: pd.DataFrame = connection.query(f"SHOW {table_name};").fetchdf()
        if tbl_df.empty:
            create_stmt = f"CREATE OR REPLACE TABLE {table_name}("
            for row in df_tmp_sql.itertuples(index=False):
                create_stmt += f"{row.column_name} {row.column_type}, "
            create_stmt += f" PRIMARY KEY({",".join(primary_keys)}))"            
            connection.query(create_stmt)
        else:        
            table_cols = tbl_df["column_name"].tolist() 
             
            for row in df_tmp_sql.itertuples(index=False):
                if row.column_name not in table_cols:
                    alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {row.column_name} {row.column_type};"
                    connection.query(alter_stmt)
        df_cols_stmt = ",".join(df.columns.tolist())
        
        connection.query(f"INSERT INTO {table_name}({df_cols_stmt}) SELECT {df_cols_stmt} FROM df")
        
    finally:
        connection.sql("DROP TABLE temp_sql;")
        
if __name__ == "__main__":
    url = "http://test-public-storage-440955376164.s3-website.us-east-1.amazonaws.com/catalogs/demo.db"
    downloaded = download(url)        