#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    gzip_name = 'output.gz'
    csv_name = 'output.csv'

    os.system(f'wget {url} -O {csv_name}')
    #os.system(f'wget {url} -O {gzip_name}')
    #os.system(f'gzip -d -c {gzip_name} > {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100_000)


    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    
    while True:
        t_start = time()
        
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunk..., took %.3f second' % (t_end - t_start))
        


if __name__ == '__main__':
        
    parser = argparse.ArgumentParser(
                        description = 'Ingest CSV data to Postrgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the result to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)

