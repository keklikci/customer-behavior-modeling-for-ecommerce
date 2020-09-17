#!/usr/bin/env python
# coding: utf-8

# create spark session & context
from pyspark.sql import SparkSession
# import dask.dataframe as dd

def main():
    
    spark = SparkSession \
    .builder \
    .appName("split_Input")  \
    .getOrCreate()

    sdf = spark.read.parquet("/data/insider/largedata.parquet") 
    sdf.coalesce(13).write.parquet("/data/insider/partitioned")

    """
    ddf = dd.read_parquet('/data/insider/largedata.parquet')
    ddf.repartition(3).to_parquet('/data/insider/partitioned')
    """
if __name__ == "__main__":
    main()

