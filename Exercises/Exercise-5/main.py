import psycopg2
import pandas as pd

from sqlalchemy import create_engine

host = "postgres"
database = "postgres"
user = "root"
pwd = "password"

def main():
    # Create tables
    conn = psycopg2.connect(host=host, database=database, user=user, password=pwd)
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(open('database/schema.sql', 'r').read())

    # Reset data
    with conn.cursor() as cursor:
        cursor.execute(open('database/clear_data.sql', 'r').read())

    # Insert data
    db = create_engine(f'postgresql://{user}:{pwd}@{host}/{database}')
    sqlconn = db.connect()

    df = pd.read_csv('data/accounts.csv', parse_dates=True)
    df = df.rename(columns=lambda x: x.strip())
    df.to_sql('account', con=sqlconn, if_exists='append', index=False)

    df = pd.read_csv('data/products.csv')
    df = df.rename(columns=lambda x: x.strip())
    df.to_sql('product', con=sqlconn, if_exists='append', index=False)

    df = pd.read_csv('data/transactions.csv', parse_dates=True)
    df = df.rename(columns=lambda x: x.strip())
    df = df.drop(['product_code', 'product_description'], axis=1)
    df.to_sql('transaction', con=sqlconn, if_exists='append', index=False)

    # Check data
    with conn.cursor() as cursor:
        print_table(cursor, 'account')
        print_table(cursor, 'product')
        print_table(cursor, 'transaction')

def print_table(cursor, table_name):
    cursor.execute(f'SELECT * FROM {table_name}')
    for row in cursor.fetchall():
        print(row)

if __name__ == "__main__":
    main()
