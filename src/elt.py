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



class ELT:
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
        #Lets begin by creating the databases for the datasets
        self.dfLaborELT = pd.read_csv('datasets/world_labor_productivity.csv', delimiter=',')
        self.dfSalaryELT = pd.read_csv('datasets/world_annual_wage.csv', delimiter=',')
        self.dfMentalELT=pd.read_csv('datasets/mental_illness.csv', delimiter=',')
        self.dfCostELT = pd.read_csv('datasets/cost_of_living.csv', delimiter=',')

    def load(self):
        #Lets add a flag to the salary dataset in order to know if it already has been converted to USD
        self.dfSalaryELT['Converted'] = 0

        #Bug its inserting the same year multiple times

        #Lets insert the data into buckets
        self.couchbaseUsername, self.couchbasePassword = os.getenv('COUCHBASE_USERNAME'), os.getenv('COUCHBASE_PASSWORD')

        # get a reference to our cluster
        auth = PasswordAuthenticator(self.couchbaseUsername, self.couchbasePassword)
        self.cluster = Cluster.connect('couchbase://127.0.0.1', ClusterOptions(auth))

        datasetsELT = [("cost_of_living_elt", self.dfCostELT),
                    ("world_labor_productivity_elt", self.dfLaborELT),
                        ("world_annual_wage_elt",self.dfSalaryELT),
                        ("mental_illness_elt", self.dfMentalELT)]

        payloadsConstructors = [
            {"Country": "Country", "Year": "Date", "CoL": "Cost of Living", "Rent Index": "Rent Index", "Cost of Living Plus Rent Index": "Cost of Living Plus Rent Index", "Groceries Index": "Groceries Index", "Restaurant Price Index": "Restaurant Price Index", "Local Purchasing Power Index": "Local Purchasing Power Index"},
            {"Country": "Entity", "Year": "Year", "Productivity": "Productivity: output per hour worked"},
            {"Country": "Country", "Year": "Time", "Salary": "Value", "Unit Code": "Unit Code", "Converted":"Converted", "Country Code": "COUNTRY"},
            {"Country": "Entity", 
            "Year": "Year",
            "DALYs from depressive disorders per 100,000 people": "DALYs from depressive disorders per 100,000 people in, both sexes aged age-standardized", 
            "DALYs from schizophrenia per 100,000 people": "DALYs from schizophrenia per 100,000 people in, both sexes aged age-standardized", 
            "DALYs from bipolar disorder per 100,000 people": "DALYs from bipolar disorder per 100,000 people in, both sexes aged age-standardized", 
            "DALYs from eating disorders per 100,000 people": "DALYs from eating disorders per 100,000 people in, both sexes aged age-standardized", 
            "DALYs from anxiety disorders per 100,000 people": "DALYs from anxiety disorders per 100,000 people in, both sexes aged age-standardized"}
        ]

        self.db_populator(datasetsELT, payloadsConstructors)
        self.updateSalaries()
            


    def dropAllBuckets(self):
        for bucket in self.cluster.buckets().get_all_buckets():
            if bucket.name.endswith("_elt"):
                self.cluster.buckets().drop_bucket(bucket.name)
                print(f"Bucket {bucket.name} dropped")


    #TODO: Verify why its inserting too many documents in the bucket

    #Datasets structure:
    #[(bucket_name, dataset)]
    def db_populator(self, datasets, payloadConstructors, options=None):
        for i in range(len(datasets)):
            payloadConstructor = payloadConstructors[i]
            i = datasets[i]

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

            except Exception as e:
                if "Bucket with given name already exists" in str(e):
                    pass

            #Lets create a new collection to store the data
            time.sleep(2)
            bucket = self.cluster.bucket(bucketName)
            collection = bucket.default_collection()

            #Lets store the data in the collection
            #Lets get the last id in the bucket
            k=0
            for row in dataset.iterrows():
                data = row[1].to_dict()
                dataToInsert = payloadConstructor.copy()
                for key in payloadConstructor:
                    dataToFind = payloadConstructor[key]
                    if dataToFind in data:
                        dataToInsert[key] = data[dataToFind]
                
                #Rever se este id é necessário visto que depois corremos o create_primary_index
                data["_id"] = f"{k}"
                collection.upsert(str(k), dataToInsert)
                k+=1

            print('Data stored')
            #Lets create the index for the data
            try:
                self.cluster.query_indexes().create_primary_index(bucketName)
                print('Index created')
            except Exception as e:
                if "already exists" in str(e):
                    pass    

    def transform(self):

        #Probably should clean up the data here
        #Lets start for the salary dataset
        #Lets remove the rows that have no country code
        self.cluster.query("DELETE FROM `world_annual_wage_elt` WHERE `Country` IS NULL")
        #Lets remove the rows that have no year
        self.cluster.query("DELETE FROM `world_annual_wage_elt` WHERE `Year` IS NULL")
        #Lets remove the rows that have no salary probably can do some data engineering and interpolate them
        self.cluster.query("DELETE FROM `world_annual_wage_elt` WHERE Salary IS NULL")
        #Lets remove the rows that have no currency code
        self.cluster.query("DELETE FROM `world_annual_wage_elt` WHERE `Unit Code` IS NULL")
        #Lets set the flag to 0
        self.cluster.query("UPDATE `world_annual_wage_elt` SET `Converted` = 0")

        #TODO: With pycountry fix the countries that dont have the country code

        #We will ommit this since the api is paid
        #self.exchangeELT()

        #Lets create a new table to store the data from the currency conversion function
    def exchangeELT(self):
        API_KEY = os.getenv("EXAPI_KEY")

        #Probably we should start by verifying if the bucket to store the exchange rates already exists

        #Lets get all the countries currency and all the years from the datasets
        #Verify this since we dont have country codes for all the countries
        #Probably clean up the countries bucket and if any of them doesnt have coutry code remove them
        #Alter this to query the database
        countries = self.cluster.query("SELECT DISTINCT `Unit Code` FROM `world_annual_wage_elt`")
        years = self.cluster.query("SELECT DISTINCT Year FROM `world_annual_wage_elt`")


        #Lets get a string with all the country codes
        countryCodes = ",".join([row["Unit Code"] for row in countries if row != {}])

        years = [row["Year"] for row in years if row!={}]
        #Lets create a bucket to store the data
        bucketSettings = CreateBucketSettings(name="currency_exchange",
                                                bucket_type="couchbase",
                                                ram_quota_mb=100,
                                                max_expiry=0,
                                                compression_mode="passive",
                                                conflict_resolution_type="seqno")
        
        try:
            self.cluster.buckets().create_bucket(bucketSettings, name="currency_exchange")
            print('Bucket created')
            self.cluster.wait_until_ready(timedelta(seconds=20))
            #Create the primary index
            self.cluster.query_indexes().create_primary_index("currency_exchange")
        
        except Exception as e:
            if "Bucket with given name already exists" in str(e):
                pass
        
        bucket = self.cluster.bucket("currency_exchange")
        collection = bucket.default_collection()

        i=0
        
        for year in years:
            apiUrl = f"http://api.exchangeratesapi.io/v1/{str(year)}-12-12?access_key={API_KEY}&symbols=USD,{countryCodes}"
            response = requests.get(apiUrl)
            if response.status_code !=200:
                break
            exchangeRates = response.json()
            if exchangeRates['success']:
                dataToInsert = exchangeRates['rates']
                dataToInsert["Year"] = year
                #TODO: FIX Exchange rate = USD*exchangeRates['rates'][countryCode]
                collection.upsert(str(i), dataToInsert)
        
            print("Inserted exchange rates for year "+str(year))
            i+=1
        print("Exchanged successfully")


    def updateSalaries(self):
        #Lets use the exchanged data currencies to alter the values of the salary in the wage bucket
        #Lets create a function in the database to do this
        #COUCHBASE DOESNT HAVE TRIGGERS PROBABLY WILL HAVE TO CREATE A PROCESS THAT LISTENS FOR UPDATES ON THE CURRENCY EXCHANGE DATBASE
        #AND THEN ALTER THE VALUES IN THE WAGE BUCKET
        #Lets create a function to do this
        #Query the wage database and get the salary collumn and the flag collumn 
        #if the flag is not set then query the currency exchange database and alter the salary value and alter the flag value

        wageQuery = f"SELECT Salary, Converted, `Unit Code`, Year FROM `world_annual_wage_elt`"
        exchangeQuery = f"SELECT * FROM `currency_exchange` WHERE Year = $1"
        updateQuery = f"UPDATE `world_annual_wage_elt` SET Salary = $1, Converted = 1 WHERE Salary = $2 AND `Unit Code` = $3 AND Year = $4 AND Converted = 0"
        wageQueryResult = self.cluster.query(wageQuery)


        for row in wageQueryResult:
            if row!={} and row['Converted'] == 0:
                exchangeQueryResult = self.cluster.query(exchangeQuery, row['Year'], row['Unit Code'])

                Unit = row['Unit Code']
                
                eResult = None

                for eRow in exchangeQueryResult:
                    eResult = eRow
                    break

                if  Unit is not None and eResult is not None and Unit in eResult["currency_exchange"] :
                    newSalary = row['Salary'] / eResult['currency_exchange'][Unit]
                    previousSalary = row["Salary"]
                    Year = row["Year"]
                    updateQuery = f"UPDATE `world_annual_wage_elt` SET Salary = {newSalary}, Converted = 1 WHERE Salary = {previousSalary} AND `Unit Code` = \"{Unit}\" AND Year = {Year} AND Converted = 0"
                    #The update query has some delay
                    self.cluster.query(updateQuery,QueryOptions(metrics=True))
        
        print("Salaries converted to USD successfully")



    def run(self):
        print("Extraction Phase")
        self.extract()
        print("Load Phase")
        self.load()
        print("Transform Phase")
        self.transform()