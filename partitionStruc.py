#!/usr/bin/env python
# coding: utf-8

# create spark session & context
from pyspark.sql import SparkSession

# for machine count
import argparse

"""
*** << header indexes >> 
"""
DATE = 0
REFERRER_URL = 1
CURRENT_URL = 2
PAGE_TYPE = 3
PRODUCT_PRICE = 4
CART_AMOUNT = 5
USERID = 6
SESSIONID = 7
SEARCH_WORDS = 8
OLD_PRODUCT_PRICE = 9
PRODUCT_CATEGORY = 10
PAGE_CATEGORY = 11
PRODUCT_ID = 12

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('numberOfMachines', type= int)
    parser.add_argument('inputFile')
    args = parser.parse_args()
    numberOfMachines = vars(args).get('numberOfMachines')
    fname = vars(args).get('inputFile')

    spark = SparkSession \
    .builder \
    .appName("partitions_" + fname) \
    .getOrCreate()

    """ read spark dataframe """
    sdf = spark.read.parquet(fname)
   
    """ 
    *** cast string date to timestamp
    """
    from pyspark.sql.types import TimestampType
    sdf = sdf.withColumn("date",sdf["date"].cast(TimestampType()))

    sessions = sdf.rdd.map(lambda row: [row]
                               ).keyBy(lambda row: row[DATE][USERID]
                                      )
    """ 
    check =  sessions.repartition(200).glom().map(len).collect()
    print(f"Min partition size: {min(l)}\n")
    print(f"Max partition size: {max(l)}\n")
    """
    res = sessions.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    print("RESULTS BEFORE REPARTITONING")
    print(f"Max. PartitionSize: {max(res)}\tMin. PartitionSize: {min(res)}\n")
    res = sessions.repartition(200).mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    print("RESULTS AFTER  REPARTITONING")
    print(f"Max. PartitionSize: {max(res)}\tMin. PartitionSize: {min(res)}")
    """
    for i, psize in enumerate(res):
    	print(f"Partition {i}: {psize}\n")
    """

if __name__ == '__main__':
    main()
