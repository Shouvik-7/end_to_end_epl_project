from etl.utils import get_object, load_object, data_from_warehouse
from db.table_creation import create_table
import pandas as pd
from datetime import datetime


def transform_data(**kwargs):
    season = ''
    if datetime.today().month < 8:
        season = str(datetime.today().year - 2000 - 1) + str(datetime.today().year - 2000)
    else:
        season = str(datetime.today().year - 2000) + str(datetime.today().year - 2000 + 1)

    path = f'raw/epl_{str(season)}.csv'
    data = get_object(path)
    field_names = ['Date', 'Time', 'season', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'HTHG', 'HTAG', 'HTR',
                   'Referee', 'HS', 'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR']
    data = data[field_names]
    load_object(data, f'transformed/epl_{data["season"].iloc[0]}.csv')

