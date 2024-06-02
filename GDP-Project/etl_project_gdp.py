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

#Extract
log_progress("Extract phase Started") 

# Define the URL and database details
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = '/home/project/top_50_films.csv'
target_file = "Countries_by_GDP.json" 


# Fetch the HTML content
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

# Find the table
table = data.find('table', {'class': 'wikitable'})

# Initialize an empty DataFrame
df = pd.DataFrame(columns=["Country", "GDP_USD_billion"])

# Extract data from the table
for row in table.find_all('tr')[1:]:  # Skip the header row
    cols = row.find_all('td')
    if len(cols) >= 4:
        
        country = cols[0].get_text(strip=True)
        gdp_million = cols[2].get_text(strip=True).replace(',', '')

        if gdp_million == '—':
            gdp_million = '0'

        data_dict = {"Country": country,
                         "GDP_USD_billion": float(gdp_million)}
        

        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)

log_progress("Extract phase Ended") 

log_progress("Transform phase Started") 

#Transform
def transform(data): 
    '''GDPs in billion USDs (rounded to 2 decimal places. 
    Dividing by 1000 to convert million to billion'''

    data['GDP_USD_billion'] = round(data.GDP_USD_billion/1000,2) 
    
    return data 


transformed_data = transform(df) 
print("Transformed Data") 
print(transformed_data) 

log_progress("Transform phase Ended") 

log_progress("Loading phase Started") 

#Load to database
conn = sqlite3.connect(db_name)
transformed_data.to_sql(table_name, conn, if_exists='replace', index=False)

# Query displaying only the entries with more than a 100 billion USD economy
query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billion > 100"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()


#Load to JSON file
transformed_data.to_json(target_file) 

log_progress("Loading phase Ended") 

log_progress("ETL Job Ended") 

# print(df)
