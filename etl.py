import pandas as pd;
import numpy as np 
import pycountry as pc
from pyspark.sql import SparkSession

import matplotlib.pyplot as plt
import seaborn as sns


from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.bucket import Bucket
from couchbase.management.buckets import CreateBucketSettings

import os
from dotenv import load_dotenv
from datetime import datetime 
from datetime import timedelta
import time
import requests

### Couchbase db setup
load_dotenv(override=True)



class ETL:
    def __init__(self):
        #TODO implement the CLI and add an option to delete all buckets
        pass

    def benchmark(self):
        start_time = datetime.now()
        self.run()    
        end_time = datetime.now()
        print(f"Execution time: {end_time - start_time}")
        return end_time - start_time


    def extract(self):
        #DATASETS ESCOLHIDOS

        self.dfLabor = pd.read_csv('datasets/world_labor_productivity.csv', delimiter=',')
        #To change for the original dataset
        self.dfSalary = pd.read_csv('datasets/world_annual_wage.csv', delimiter=',')
        self.dfExchangeRates = pd.read_csv('datasets/exchange_rates.csv', delimiter=',')
        self.dfMental=pd.read_csv('datasets/mental_illness.csv', delimiter=',')
        self.dfCost = pd.read_csv('datasets/cost_of_living.csv', delimiter=',')
                

    def transform(self):
        self.dfMental.rename(columns={'Entity':'Country'}, inplace=True)
        self.dfMental.rename(columns={'DALYs from depressive disorders per 100,000 people in, both sexes aged age-standardized':'DALYs from depressive disorders per 100,000 people'}, inplace=True)
        self.dfMental.rename(columns={'DALYs from bipolar disorder per 100,000 people in, both sexes aged age-standardized':'DALYs from bipolar disorder per 100,000 people'}, inplace=True)
        self.dfMental.rename(columns={'DALYs from schizophrenia per 100,000 people in, both sexes aged age-standardized':'DALYs from schizophrenia per 100,000 people'}, inplace=True)
        self.dfMental.rename(columns={'DALYs from eating disorders per 100,000 people in, both sexes aged age-standardized':'DALYs from eating disorders per 100,000 people'}, inplace=True)
        self.dfMental.rename(columns={'DALYs from anxiety disorders per 100,000 people in, both sexes aged age-standardized':'DALYs from anxiety disorders per 100,000 people'}, inplace=True)

        self.dfLabor.rename(columns={"Entity": "Country"}, inplace=True)
        self.dfLabor.drop(columns=['Code'], inplace=True)
        self.dfLabor.rename(columns={'Productivity: output per hour worked':'Productivity'}, inplace=True)

        
        # #COUNTRY,"Country","SERIES","Series","TIME","Time","Unit Code","Unit","PowerCode Code","PowerCode","Reference Period Code","Reference Period","Value","Flag Codes","Flags"
        self.dfSalary.drop(columns=[ 'COUNTRY' , "Flag Codes", "Flags", "SERIES", "Series", "Unit", "PowerCode", "PowerCode Code","Reference Period Code", "Reference Period"], inplace=True)
        self.dfSalary.rename(columns={'Value':'Salary'}, inplace=True)
        self.dfSalary.rename(columns={'PowerCode':'Unit Code'}, inplace=True)
        self.dfSalary.rename(columns={'Time':'Year'}, inplace=True)
        self.dfSalary.drop(columns=[ 'TIME' ], inplace=True)
        #Lets get all countries that have no unit code
        countriesWithoutUnitCode = self.dfSalary[self.dfSalary['Unit Code'].isna()]["Country"].unique()
        #Drop the countries without unit code
        self.dfSalary = self.dfSalary[~self.dfSalary['Country'].isin(countriesWithoutUnitCode)]
        self.dfSalary = self.dfSalary.dropna(subset=['Year'])
        self.dfSalary = self.dfSalary.dropna(subset=['Salary'])
        self.dfSalary = self.dfSalary.dropna(subset=['Unit Code'])
        self.dfSalary = self.dfSalary.dropna(subset=['Country'])

        self.dfCost.rename(columns={'Date':'Year'}, inplace=True)
        self.dfCost.rename(columns={'Cost of Living':'CoL'}, inplace=True)

        #Lets ommit this part since the api is paid and we dont need to spend more credits on this
        # self.getExchangeRates()
        self.exchange()

    def load(self):
                #Should we merge the collumns Rent index and Groceries index and rename it to Basic Needs Index and the Restaurant Index to Leisure Index
        self.couchbaseUsername, self.couchbasePassword = os.getenv('COUCHBASE_USERNAME'), os.getenv('COUCHBASE_PASSWORD')


        # get a reference to our cluster
        auth = PasswordAuthenticator(self.couchbaseUsername, self.couchbasePassword)
        self.cluster = Cluster.connect('couchbase://127.0.0.1', ClusterOptions(auth))
        
        self.db_populator([("cost_of_living_etl", self.dfCost), 
                ("world_labor_productivity_etl", self.dfLabor), 
                ("world_annual_wage_etl", self.dfSalary), 
                ("mental_illness_etl", self.dfMental)])
        


    #Datasets structure:
    #[(bucket_name, dataset)]
    def db_populator(self, datasets, options=None):
        for i in datasets:
            bucketName, dataset = i
            if options!=None:
                bucketSettings = CreateBucketSettings(name=options["name"], 
                                                    bucket_type=options["bucket_type"], 
                                                    ram_quota_mb=options["ram_quota_mb"], 
                                                    max_expiry=options["max_expiry"], 
                                                    compression_mode=options["compression_mode"], 
                                                    conflict_resolution_type=options["conflict_resolution_type"])
            else:
                bucketSettings = CreateBucketSettings(name=bucketName, 
                                                    bucket_type="couchbase", 
                                                    ram_quota_mb=100, 
                                                    max_expiry=0, 
                                                    compression_mode="passive", 
                                                    conflict_resolution_type="seqno")
                
            
            print(f"Populating {bucketName} with {len(dataset)} documents")
            #Create the bucket to store the files
            try:
                
                self.cluster.buckets().create_bucket(bucketSettings, name=f'{bucketName}')
                print('Bucket created')
                self.cluster.wait_until_ready(timedelta(seconds=20))
                #TODO: Rever isto, por vezes parte visto que pode demorar mais a criar o bucket

            except Exception as e:
                if "Bucket with given name already exists" in str(e):
                    pass

            time.sleep(2)
            #Lets create a new collection to store the data
            bucket = self.cluster.bucket(bucketName)
            collection = bucket.default_collection()

            #Lets store the data in the collection
            #Lets store the data in the collection
            #Lets get the last id in the bucket
            i=0
            for row in dataset.iterrows():
                data = row[1].to_dict()
                #Rever se este id é necessário visto que depois corremos o create_primary_index
                data["_id"] = f"{i}"
                collection.upsert(str(i), data)
                i+=1

            print('Data stored')
            #Lets create the index for the data
            try:
                self.cluster.query_indexes().create_primary_index(bucketName)
                print('Index created')
            except Exception as e:
                if "already exists" in str(e):
                    pass    


    def getExchangeRates(self):
        API_KEY = os.getenv("EXAPI_KEY")

        # Lets use the Unit Code to get the exchange rates for each country
        # Lets collect all the Unit Codes for the countries per year
        unitCodesPerYear = {}
        for year in self.dfSalary['Year'].unique():
            unitCodesPerYear[year] = self.dfSalary[self.dfSalary['Year'] == year]['Unit Code'].unique().tolist()


        #Lets create a new dataset to store this information
        dfExchangeRates = pd.DataFrame(columns=['Year', 'Unit Code', 'Exchange Rate'])

        for year in unitCodesPerYear:
            
            unitCodes = ",".join([unit for unit in unitCodesPerYear[year] if str(unit)!="nan"])
            apiUrl = f"http://api.exchangeratesapi.io/v1/{str(year)}-12-12?access_key={API_KEY}&symbols=USD,{unitCodes}"
            response = requests.get(apiUrl)
            if response.status_code !=200:
                break
            exchangeRates = response.json()
            
            for index, row in self.dfSalary[self.dfSalary['Year'] == year].iterrows():
                unitCode = row['Unit Code']
                if unitCode in exchangeRates['rates']:
                    exchangeRate = exchangeRates['rates'][unitCode]
                    dfExchangeRates = pd.concat([dfExchangeRates, pd.DataFrame({'Year': [year], 'Unit Code': [unitCode], 'Exchange Rate': [exchangeRate]})], ignore_index=True)
                else:
                    latest_exchange_rate = None
                    for code, rate in exchangeRates['rates'].items():
                        if code in unitCodesPerYear[year]:
                            latest_exchange_rate = rate
                            break
                    if latest_exchange_rate is not None:
                        dfExchangeRates = pd.concat([dfExchangeRates, pd.DataFrame({'Year': [year], 'Unit Code': [unitCode], 'Exchange Rate': [latest_exchange_rate]})], ignore_index=True)
        
        #Lets save the dataframe with the exchange rates 
        dfExchangeRates.to_csv('datasets/exchange_rates.csv', index=False)
        print("Exchanged successfully")

    def exchange(self):
        #Verify if this makes the changes in the saved csv for this dataset
        for row in self.dfSalary.iterrows():
            index, row = row
            entity = row['Country']
            unitCode = row['Unit Code']
            salary = row['Salary']
            year = row['Year']
            exchangeRate = self.dfExchangeRates[(self.dfExchangeRates['Year'] == year) & (self.dfExchangeRates['Unit Code'] == unitCode)]['Exchange Rate']
            if len(exchangeRate) > 0:
                exchangeRate = exchangeRate.iloc[0]
                salaryInUSD = salary / exchangeRate
                self.dfSalary.at[index, 'Salary'] = salaryInUSD
                self.dfSalary.at[index, 'Exchange Rate'] = float(exchangeRate)
            else:
                print(f"Exchange rate not found for {entity} in {year}")


    def run(self):
        print("Extraction Phase")
        self.extract()
        print("Transforming Phase")
        self.transform()
        print("Loading Phase")
        self.load()

    def dropAllBuckets(self):
        for bucket in self.cluster.buckets().get_all_buckets():
            if bucket.name.endswith("_etl"):
                self.cluster.buckets().drop_bucket(bucket.name)
                print(f"Bucket {bucket.name} dropped")
