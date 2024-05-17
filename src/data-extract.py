from helpers import * 
import logging 
from dotenv import dotenv_values 
import tempfile 
import os 
import pandas as pd 
from sqlalchemy import create_engine

tokens = ['INJ', 'QNT', 'STORJ', 'VELO', 'SOL', 'JTO', 'ICP', 'SHIB', 'AUCTION', 'OCEAN', 'BONK','TIME', 'BTC', 'JUP','ILV']

#extract data from API


#extract historical data


#extract cutrrent stats 


#save data to s3
"""
1 year historical data 
"""
def save_to_s3(filename, folder):
    s3 = s3_client()
    try:
        s3.upload_file(f'{filename}.zip', 'elias-crypto-data', f'{folder}/{filename}.zip') 
    except Exception as e:
        logging.info(f'Error {e} occured!') 


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


#copy data from s3 to redshift 
def s3_to_redshift(filename):
    s3 = s3_client()

    sql_stmt = """
    COPY {table_name}
    FROM 's3://{bucket_name}/{object_key}'
    {delimiter}
    CSV {header_handling}
    ACCEPTINVCHARS;
""".format(table_name=env['REDSHIFT_TABLE'], bucket_name=env['S3_BUCKET_NAME'], object_key=f'rawdata/{filename}.zip')
    
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

        # Use COPY command on the extracted data file
        # Update SQL statement with delimiter and header handling
        #sql_stmt = sql_stmt.format(delimiter=delimiter, header_handling=header_handling)
        execute_copy_from_local_file(sql_stmt, extracted_file_path)


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
def get_transformations(data, asset):
    """
    takes in a json data and creates a pandas dataframe
    """
    now = datetime.now() 
    seven_day = now - timedelta(days=7) 
    thirty_day = now - timedelta(days=30)

    seven_timestamp = int(seven_day.timestamp())
    thirty_timestamp = int(thirty_day.timestamp())

    current_stats = get_token_stats(asset) 

    seven_df = data[data['Time'] == seven_timestamp]
    thirty_df = data[data['Time'] == thirty_timestamp] 

    #more to do 
    seven_day_discount = current_stats['last'] - seven_df['Close'][0]
    thirty_day_discount = current_stats - thirty_df['Close'][0] 

    #calculate percentage discount
    


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



def main():
    start = datetime.strptime('2024-01-01', '%Y-%m-%d').strftime('%Y-%m-%d')
    end = datetime.now().strftime('%Y-%m-%d')

    for asset in tokens:
        #get historical data for asset 
        data = get_historical_data(asset, start, end) 
        
        #convert asset to zipfile
        filename = f'{asset}_start:end'
        data.to_csv(f'../Data/{filename}.csv')
        to_zipfile(f'{filename}.csv', f'{filename}.zip') 

        #get s3 client 
        s3 = s3_client()
        #upload zipfile to s3 
        save_to_s3(filename, 'rawdata')
 
        #get data from s3 to redshift 
        s3_to_redshift(filename)

        #get data from s3 to postgres
        s3_to_postgres(filename)
