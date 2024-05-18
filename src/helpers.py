import requests 
from datetime import datetime, timedelta
import pandas as pd
import time
import boto3 
from dotenv import dotenv_values
import zipfile
import logging
import psycopg2 # type: ignore
import io 
from sqlalchemy import create_engine
import os 
import tempfile


env = dotenv_values('../.env')

# Create a session for HTTP requests
session = requests.Session()
session.headers.update({
    'Content-Type': 'application/json',
    'User-Agent': 'Python http.client'
})

def get_json_response(url):
    """Utility function to send GET request and return JSON response."""
    try:
        response = session.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        return response.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def get_token_stats(token):
    url = f"https://api.exchange.coinbase.com/products/{token}-USD/stats"
    data = get_json_response(url)
    return data
    
def s3_client():
    s3 = boto3.client('s3',
                  aws_access_key_id = env.get('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=env.get('AWS_SECRET_ACCESS_KEY')) 
    
    return s3 

def redshift_client():
    conn = psycopg2.connect(
        host=env['REDSHIFT_HOST'],
        port=env['REDSHIFT_PORT'], 
        database=env['REDSHIFT_DATABASE'],
        user=env['REDSHIFT_USER'], 
        password=env['REDSHIFT_PASSWORD']
    )
    return conn 

def postgres_con():
    # Connect to PostgreSQL
    conn = psycopg2.connect(host= env['DB_HOST'], 
                            port= env['DB_PORT'], 
                            database= env['DB_NAME'], 
                            user= env['DB_USER'], 
                            password= env['DB_PASSWORD'])
    return conn 

def to_zipfile(csv_file):
    zip_file_path = csv_file.split()[0] + '.zip'
    with zipfile.ZipFile(zip_file_path, "w") as zip_file:
        zip_file.write(csv_file, os.path.basename(csv_file))      

    return zip_file_path  


#copy localfiles to s3
def execute_copy_from_local_file(sql_stmt, local_file_path):
    conn = redshift_client()
    cur = conn.cursor()

    try:
    # Execute COPY command with the local file path
        with open(local_file_path, 'r') as f:
            cur.copy_expert(sql=sql_stmt, file=f)
        conn.commit()
        logging.info(f"Successfully loaded data from S3 to Redshift table {env['REDSHIFT_TABLE']}")

    except Exception as e:
        logging.error(f"Error loading data into Redshift: {e}")
        conn.rollback()  # Rollback on error

    finally:
        # Close connections
        cur.close()
        conn.close()

def download_data_from_s3(filename):
    s3 = s3_client()
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_file_path = os.path.join(tmp_dir, "data.zip")
        try:
            s3.download_file(env['S3_BUCKET_NAME'], f'rawdata/{filename}.zip', local_file_path)
        except Exception as e:
            if e.response['Error']['Code'] == "404":
                logging.error(f"Error: File {filename}.zip not found in S3 bucket {env['S3_BUCKET_NAME']}")
                return
            else:
                raise

        # Extract the data file from the zip (assuming there's only one)
        with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
            extracted_file_name = zip_ref.namelist()[0]  # Assuming single file
            extracted_file_path = os.path.join(tmp_dir, extracted_file_name)
            zip_ref.extractall(tmp_dir)

    return extracted_file_path

#copy data from s3 to redshift 
def s3_to_redshift(filename):

    sql_stmt = """
    COPY {table_name}
    FROM 's3://{bucket_name}/{object_key}'
    {delimiter}
    CSV {header_handling}
    ACCEPTINVCHARS;
    """.format(table_name=env['REDSHIFT_TABLE'], bucket_name=env['S3_BUCKET_NAME'], object_key=f'rawdata/{filename}.zip')
    
    
    extracted_file_path = download_data_from_s3(filename)
    # Use COPY command on the extracted data file
    # Update SQL statement with delimiter and header handling
    #sql_stmt = sql_stmt.format(delimiter=delimiter, header_handling=header_handling)
    execute_copy_from_local_file(sql_stmt, extracted_file_path)

def s3_to_postgres(filename):
    # Connect to S3
    s3 = s3_client()

    # Download the zip file to a memory buffer
    buffer = io.BytesIO()
    try:
        s3.download_fileobj(env['S3_BUCKET_NAME'], f'rawdata/{filename}.zip', buffer)
    except Exception as e:
        if e.response['Error']['Code'] == "404":
            logging.error(f"Error: File f'rawdata/{filename}.zip' not found in S3 bucket {env['S3_BUCKET_NAME']}")
            return
        else:
            raise

   
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
    # Assuming there's only one data file inside the zip
        # Assuming there is only one CSV file in the zip
        csv_file_name = zip_ref.namelist()[0]
        with zip_ref.open(csv_file_name) as csv_file:
            # Read CSV file into pandas DataFrame
            df = pd.read_csv(csv_file)

    # Connect to PostgreSQL
    postgres_url = f'postgresql://{env['DB_USER']}:{env['DB_PASSWORD']}@{env['DB_HOST']}:{env['DB_PORT']}/{env['DB_NAME']}'
    engine = create_engine(postgres_url)

    # Save DataFrame to PostgreSQL (assuming you want to replace the existing table)
    table_name = 'your_table_name'
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    logging.info("CSV data has been successfully saved to the PostgreSQL database.")

#transform data in redshift 
"""
create a 7d discount table 
    get current stats 
    get stats 7 days ago 
    calculate oercentage change 
    return top 5 discounted coins 

create 30d discount table 
    get current stats
    get stats 30 days ago
    calculate percentage change 
    return the top5 discounted coins
"""
