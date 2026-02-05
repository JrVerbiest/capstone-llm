import argparse
import logging
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):

    #df_questions = spark.read.json('data/questions.json')
    #df_answers = spark.read.json('data/answers.json')

    df_questions = spark.read.json(f's3a://dataminded-academy-capstone-llm-data-us/input/{tag}/questions.json')
    df_answers = spark.read.json(f's3a://dataminded-academy-capstone-llm-data-us/input/{tag}/answers.json')
    
    questions_df = df_questions.select(explode("items").alias("item")).select("item.*")
    answers_df = df_answers.select(explode("items").alias("item")).select("item.*")
    
    questions_clean = questions_df.select(
        col("question_id"),
        col("title"),
        col("body").alias("question_body")
    )
    
    answers_clean = answers_df.select(
        col("question_id"),
        col("body").alias("response_body")
    )
    
    joined_df = questions_clean.join(
        answers_clean,
        on='question_id'
    )
    
    # Set output directory based on environment
    output_dir = f's3a://dataminded-academy-capstone-llm-data-us/cleaned/Joeri/{tag}'
    
    # Write to S3 using PySpark's native JSON writer
    # This creates one JSON file per partition
    # joined_df.coalesce(1).write.mode('overwrite').json(output_dir)
    num_partitions = 4
    joined_df.repartition(num_partitions).write.mode('overwrite').json(output_dir)
    
    print(f"✓ Successfully wrote data to {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
