import pandas as pd
import requests

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup

with DAG('dag_covid_pg',
         default_args={'owner': 'airflow'},
         schedule_interval='0 9 * * *',
         start_date=days_ago(1)) as dag:
    def extract_covid_load_pg(**context):
        # return df of covid data set
        def extract_covid():
            url = 'https://www.worldometers.info/coronavirus/'
            columns = ['Country,Other', 'TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths', 'TotalRecovered', 'NewRecovered',
                       'ActiveCases']
            df = pd.DataFrame(columns=columns)

            r = requests.get(url=url)
            if r.status_code != 200:
                print('Connection Error')
            else:
                soup = BeautifulSoup(r.text, 'lxml')
                rows = soup.find('table', id='main_table_countries_today').find('tbody')
                for row in rows.find_all('tr'):
                    data = []
                    for column in row.find_all('td')[1:9]:
                        # remove '/n' in value
                        value = column.text.strip()
                        if value and value != 'N/A':
                            data.append(value)
                        else:
                            data.append(None)

                    df.loc[len(df)] = data

            return df
            # return df with remove None in Countries column, remove ':,', set numbers as int

        def clean_df(df):
            print(df)
            # remove empty value in Countries column
            df.dropna(subset=['Country,Other'], inplace=True)
            # remove ':' in str
            df[['Country,Other', 'NewCases', 'NewDeaths', 'NewRecovered']] = df[['Country,Other', 'NewCases', 'NewDeaths',
                                                                                 'NewRecovered']].replace(':', '', regex=True)
            # remove ',' in numbers
            df[['TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths', 'TotalRecovered', 'NewRecovered', 'ActiveCases']] = df[[
                'TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths', 'TotalRecovered', 'NewRecovered', 'ActiveCases']].replace(',',
                                                                                                                                '',
                                                                                                                                regex=True)
            # set int type for numbers
            df[['TotalCases', 'TotalDeaths', 'TotalRecovered', 'ActiveCases']] = df[['TotalCases', 'TotalDeaths',
                                                                                     'TotalRecovered', 'ActiveCases']].astype(
                int, errors='ignore')

            return df

        # load df into 3 tables
        def load_to_pg(df):
            pg_conn = PostgresHook(postgres_conn_id='postgres')

            for i in range(1, 4):
                table_name = f'business.table_{i}'
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                    "Country,Other" VARCHAR(50),
                    "TotalCases" INTEGER NULL,
                    "NewCases" VARCHAR(10),
                    "TotalDeaths" INTEGER NULL,
                    "NewDeaths" VARCHAR(10),
                    "TotalRecovered" INTEGER NULL,
                    "NewRecovered" VARCHAR(10),
                    "ActiveCases" INTEGER NULL
                    )
                    """
                pg_conn.run(create_table_query)
                pg_conn.insert_rows(
                    table=table_name,
                    rows=df.values.tolist())

        covid_df = clean_df(extract_covid())
        print(covid_df.head())

        load_to_pg(covid_df)


    def extract_pg_sort_load_pg(**context):
        pg_conn = PostgresHook(postgres_conn_id='postgres')
        # select and sorted by TotalRecovered / TotalCases
        query = """
            SELECT "Country,Other", "TotalRecovered", "TotalCases"
            FROM business.table_1
            ORDER BY "TotalRecovered" / "TotalCases"
            LIMIT 10
            """
        result = pg_conn.get_records(query)
        # create new table
        pg_conn.run('''
                    CREATE TABLE IF NOT EXISTS business.table_sort (
                    "Country" VARCHAR(50),
                    "TotalRecovered" INTEGER NULL,
                    "TotalCases" INTEGER NULL)
                    ''')
        # insert into new table
        pg_conn.insert_rows(table='business.table_sort',
                            rows=result,
                            target_fields=['"Country"', '"TotalRecovered"', '"TotalCases"'])


    t1 = PythonOperator(task_id='extract_covid_load_pg',
                        python_callable=extract_covid_load_pg)

    t2 = PythonOperator(task_id='extract_pg_sort_load_pg',
                        python_callable=extract_pg_sort_load_pg)

t1 >> t2