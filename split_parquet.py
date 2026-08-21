#!/usr/bin/env python
# coding: utf-8

import argparse

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Coalesce a Parquet dataset")
    parser.add_argument("input_file")
    parser.add_argument("output_directory")
    parser.add_argument("--partitions", type=int, default=13)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("split_input").getOrCreate()
    try:
        spark.read.parquet(args.input_file).coalesce(args.partitions).write.parquet(
            args.output_directory
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
