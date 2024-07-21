import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import os


def create_table():
    DB_NAME = "epl_data"
    DB_USER = "epl_data_user"
    DB_PASSWORD = "0ULiShr3jjIdlJOyFwbMOCNsjppJiWKr"
    DB_HOST = 'dpg-cqc3elg8fa8c73ckgb8g-a.oregon-postgres.render.com'
    DB_PORT = 5432

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME)

    table_queries = ["""CREATE TABLE IF NOT EXISTS epl(
    match_id serial primary key,
    "Date" VARCHAR,
    "Time" VARCHAR,
    "season" INT,
    "HomeTeam" VARCHAR,
    "AwayTeam" VARCHAR,
    "FTHG" INT,
    "FTAG" INT,
    "FTR" VARCHAR,
    "HTHG" INT,
    "HTAG" INT,
    "HTR" VARCHAR,
    "Referee" VARCHAR,
    "HS" INT,
    "AS" INT,
    "HST" INT,
    "AST" INT,
    "HF" INT,
    "AF" INT,
    "HC" INT,
    "AC" INT,
    "HY" INT,
    "AY" INT,
    "HR" INT,
    "AR" INT
    );""" ]

    cursor = conn.cursor()

    for query in table_queries:
        cursor.execute(query)
    print('Tables have been created')
    conn.commit()
    cursor.close()
    conn.close()
if __name__ == '__main__':
    create_table()