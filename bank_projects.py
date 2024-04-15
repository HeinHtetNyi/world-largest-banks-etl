from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3

URL = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
EXTRACT_ATTRIBUTES = ["Name", "MC_USD_Billion"]
FINAL_ATTRIBUTES = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
CSV_PATH = "./Largest_banks_data.csv"
LOG_PATH = "./code_log.txt"

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(LOG_PATH,"a") as f: 
        f.write(timestamp + ',' + message + '\n')
        
        
def extract():
    df = pd.DataFrame(columns=EXTRACT_ATTRIBUTES)
    html_text = requests.get(URL).text
    data = BeautifulSoup(html_text, 'html.parser')
    tbodies = data.find_all("tbody")
    desire_table = tbodies[0]
    rows = desire_table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) > 0:
            data = {
                "Name": cols[1].contents[2].string,
                "MC_USD_Billion": cols[2].string.replace("\n", "")
            }
            df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
    print(df)
    return df


def transform(df):
    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype("float64")
    df["MC_GBP_Billion"] = round(df["MC_USD_Billion"] * 0.79, 2)
    df["MC_INR_Billion"] = round(df["MC_USD_Billion"] * 83.02, 2)
    return df


def load_to_csv(df: pd.DataFrame):
    df.to_csv(CSV_PATH)


def load_to_db(df: pd.DataFrame):
    connection = sqlite3.connect(DB_NAME)
    df.to_sql(TABLE_NAME, connection, if_exists="replace", index=False)
    connection.close()
    
    
def run_query(query_statement):
    connection = sqlite3.connect(DB_NAME)
    df = pd.read_sql(query_statement, connection)
    return df


log_progress("Start extracting data")
df = extract()
log_progress("End extracting data")

log_progress("Start transforming data")
df = transform(df)
log_progress("End transforming data")

log_progress("Start loading data to csv")
load_to_csv(df)
log_progress("End loading data to csv")

log_progress("Start loading data to db")
load_to_db(df)
log_progress("End loading data to db")

sql = f"SELECT * FROM {TABLE_NAME}"
df = run_query(sql)
print(df.head())
        
        
