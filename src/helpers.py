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
    
def get_historical_data(token, start, end):
    url = f'https://api.exchange.coinbase.com/products/{token}-USD/candles?start={start}&end={end}&granularity=86400'
    data = get_json_response(url)
    data = pd.DataFrame(data, columns=['Time', 'Low', 'High', 'Open', 'Close', 'Volume']) 
    return data 

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

def to_zipfile(csv_file, output_filename):
    with zipfile.ZipFile(output_filename, "w") as zip_file:
        with zip_file.writestr("data.csv", "\n".join([",".join(row) for row in csv_file])) as csv_file:
            pass 

