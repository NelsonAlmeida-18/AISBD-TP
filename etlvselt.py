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

from elt import ELT
from etl import ETL

### Couchbase db setup
load_dotenv();

class ETLvsELT:
    def __init__(self):
        self.etl = ETL()
        self.elt = ELT()
        self.benchmark_etlvselt()

    def benchmark_etlvselt(self):
        results = []
        numRuns = 2
        for _ in range(numRuns):
            #Create a cluster instance drop all buckets
            #Lets insert the data into buckets
            self.couchbaseUsername, self.couchbasePassword = os.getenv('COUCHBASE_USERNAME'), os.getenv('COUCHBASE_PASSWORD')

            # get a reference to our cluster
            auth = PasswordAuthenticator(self.couchbaseUsername, self.couchbasePassword)
            self.cluster = Cluster.connect('couchbase://127.0.0.1', ClusterOptions(auth))
            for bucket in self.cluster.buckets().get_all_buckets():
                if bucket.name.endswith("_elt") or bucket.name.endswith("_etl"):
                    self.cluster.buckets().drop_bucket(bucket.name)
                    print(f"Dropped bucket {bucket.name}")
            
            # ETL 
            etlTime = self.etl.benchmark()
            # ELT
            eltTime = self.elt.benchmark()
            results.append((etlTime, eltTime))
        
        print(f"ETL vs ELT Benchmark over {numRuns} runs:")
        print("ETL Time (s), ELT Time (s)")
        for etlTime, eltTime in results:
            print(f"{etlTime}, {eltTime}")

        # TODO: Rever isto do int
        etl_times = [etlTime.total_seconds() for etlTime, _ in results]
        elt_times = [eltTime.total_seconds() for _, eltTime in results]
        avg_etl_time = sum(etl_times) / len(etl_times)
        avg_elt_time = sum(elt_times) / len(elt_times)
        avg_time_diff = avg_etl_time - avg_elt_time
        print(f"ETL vs ELT Benchmark (Average): {avg_time_diff}")
        print("ETL vs ELT Benchmark Completed")

ETLvsELT()