import requests
from bs4 import BeautifulSoup
from datetime import datetime
from io import BytesIO
import pandas as pd
from etl.utils import load_object

def get_data():
    # Making a GET request
    r = requests.get('https://www.football-data.co.uk/englandm.php')

    # check status code for response received
    # success code - 200
    print(r)

    # Parsing the HTML
    soup = BeautifulSoup(r.content, 'html.parser')

    a_tags = soup.find_all('a', string="Premier League")

    url_link = a_tags[0].get('href')
    season = url_link.split('/')[-2]
    req = requests.get(f"https://www.football-data.co.uk/{url_link}")
    url_content = req.content

    df = pd.read_csv(BytesIO(url_content))

    # Export the final dataframe to a CSV file
    final_df = df
    final_df['season'] = season
    path = f'raw/epl_{str(season)}.csv'
    load_object(final_df, path)


