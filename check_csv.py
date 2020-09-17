#!/usr/bin/env python
# coding: utf-8

# create spark session & context
from pyspark.sql import SparkSession

def main():
    
    spark = SparkSession \
    .builder \
    .appName("check_CSV")  \
    .getOrCreate()

    sdf = spark.read.csv("/data/insider/partitionedOutput/partition_1.csv", header=True)
    sdf.show(1,truncate=True,vertical=True) 
    
if __name__ == "__main__":
    main()

