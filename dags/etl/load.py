from etl.utils import get_object
import os
from datetime import datetime
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String


def load_data(**kwargs):
    season = ''
    if datetime.today().month < 8:
        season = str(datetime.today().year - 2000 - 1) + str(datetime.today().year - 2000)
    else:
        season = str(datetime.today().year - 2000) + str(datetime.today().year - 2000 + 1)

    path = f'transformed/epl_{str(season)}.csv'
    df = get_object(path)

    DB_NAME = "epl_data"
    DB_USER = "epl_data_user"
    DB_PASSWORD = "0ULiShr3jjIdlJOyFwbMOCNsjppJiWKr"
    DB_HOST = 'dpg-cqc3elg8fa8c73ckgb8g.oregon-postgres.render.com'
    DB_PORT = 5432

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME)

    check_query = f"DELETE FROM epl WHERE season={df['season'].iloc[0]};"

    cursor = conn.cursor()

    cursor.execute(check_query)
    print('rows deleted')
    conn.commit()
    cursor.close()
    conn.close()

    db_url = "postgresql+psycopg2://epl_data_user:0ULiShr3jjIdlJOyFwbMOCNsjppJiWKr@dpg-cqc3elg8fa8c73ckgb8g.oregon-postgres.render.com/epl_data"
    engine = create_engine(db_url)

    df.to_sql('epl', engine, index=False, if_exists='append')


