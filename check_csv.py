#!/usr/bin/env python
# coding: utf-8

import argparse

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview one CSV partition")
    parser.add_argument("input_file")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("check_csv").getOrCreate()
    try:
        spark.read.csv(args.input_file, header=True).show(
            1, truncate=True, vertical=True
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
