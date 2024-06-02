import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime 

log_file = "log_file.txt" 
#Logging data
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 


# Define the URL and database details
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Top_banks.db'
table_name = 'largest_banks'
target_file = "top_banks.csv" 


def extract():

    # Fetch the HTML content
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    # Find the table
    table = data.find('table', {'class': 'wikitable'})

    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=["Rank","Bank_Name", "Total_Asset_in_USD"])

    # Extract data from the table
    for row in table.find_all('tr')[1:]:  # Skip the header row
        cols = row.find_all('td')
        if len(cols) >= 3:
            rank = cols[0].get_text(strip=True)
            bank_name = cols[1].get_text(strip=True)
            total_asset=cols[2].get_text(strip=True).replace(',', '')

            if total_asset == '—':
                total_asset = '0'

            data_dict = {"Rank": rank,
                        "Bank_Name": bank_name,
                        "Total_Asset_in_USD": float(total_asset)}
            

            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df
   
   
#Extract
log_progress("Extract phase Started") 

df= extract()

def transform(df,GBP,EUR,INR):
    df["Total_Asset_in_GBP"] = round(df.Total_Asset_in_USD*GBP,2) 
    df["Total_Asset_in_EUR"] = round(df.Total_Asset_in_USD*EUR,2) 
    df["Total_Asset_in_INR"] = round(df.Total_Asset_in_USD*INR,2) 

    
    return df

log_progress("Extract phase Ended") 
log_progress("Transform phase Started") 

df = transform(df,0.79,0.92,83.46)
# print(df)

log_progress("Transform phase Ended") 
log_progress("Load phase Started") 

#Loading to csv file
df.to_csv(target_file) 


#Load to database
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)

# Query displaying Bank Name and Total Asset in GBP
query_statement = f"SELECT Bank_Name, Total_Asset_in_GBP FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()


log_progress("Load phase Ended") 
log_progress("ETL process Ended") 