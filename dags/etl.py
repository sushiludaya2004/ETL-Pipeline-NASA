from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


#Defining the DAG

with DAG(
    dag_id = 'nasa_postgres',
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    # Step 1 : Create a table if it does not exists
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS nasa_data (
        id serial primary key,
        title varchar(255),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type varchar(50)
        );
        """


        postgres_hook.run(create_table_query)

    # Step 2 : Extract Data
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  ## Connection ID Defined In Airflow For NASA API
        endpoint='planetary/apod', ## NASA API enpoint for APOD
        method='GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"}, ## USe the API Key from the connection
        response_filter=lambda response:response.json(), ## Convert response to json
    )

    # Step 3 : Transform Data

    @task
    def transform_nasa_data(response):
        nasa_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')

        }
        return nasa_data

    # Step 4 : Load the data into postgres SQL
    @task
    def load_data_to_postgres(nasa_data):
        ## Initialize the PostgresHook
        postgres_hook=PostgresHook(postgres_conn_id='my_postgres_connection')

        ## Define the SQL Insert Query

        insert_query = """
        INSERT INTO nasa_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        ## Execute the SQL Query

        postgres_hook.run(insert_query,parameters=(
            nasa_data['title'],
            nasa_data['explanation'],
            nasa_data['url'],
            nasa_data['date'],
            nasa_data['media_type']


        ))


    # Step 5 : Verify the data DBViewer

    # Step 6 : Define the dependency
    create_table() >> extract_apod  ## Ensure the table is create befor extraction
    api_response=extract_apod.output
    transformed_data=transform_nasa_data(api_response)
    load_data_to_postgres(transformed_data)


