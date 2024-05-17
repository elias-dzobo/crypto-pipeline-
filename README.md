# ELIAS CRYPTO DATA PIPELINE

This is an ETL pipeline that pulls historical crypto data from Coinbase.com and loads it into a Redshift data warehouse. 

## TOOLS AND SETUP
1. git 
2. Python
3. AWS account 
4. Docker 

## DATA ARCHITECTURE
![Arch](assets/crypto-pipeline.png)
## DATA PIPELINE PROCESS

### 1. DATA INGESTION
Using the Coinbase API, we gathered historical asset pricing data for over 15 different tokens and saved them individually as a zip file into an S3 bucket.

### 2. DATA TRANSFORMATION
Using the datetime module, we gathered the assets' price details 7 days and 30 days prior and calculated the price change to determine assets that were on a discount (you can buy them cheap compared to their local high prices). The transformed data was then persisted into an S3 bucket as well.

### 3. DATA LOADING 
The raw data was saved into a Redshift table and a copy saved in the PostgreSQL table as well. The data destination was modeled after the source data, meaning that we have a table that keeps the data for each asset, a separate table that stores the top 5 7-day discounted tokens, and a last table storing the top 5 30-day discounted tokens. 

## ORCHESTRATION 
Airflow was used to determine the workflow orchestration and define the data movement and dependency between the processes. 

## FUTURE WORKS 
1. Use Terraform to provision the AWS resources and use AWS RDS for production purposes. 
2. Manage workflow to run the pipeline every day to get the assets stats and update the data warehouse as well as the discount tables.
