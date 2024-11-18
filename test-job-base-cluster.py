import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

print(f"Apache Spark version: {spark.version}")

import sys
print(f"Python version: {sys.version}")

from pip import _internal
print(f"pip version: {_internal.main(['show', 'pip'])}")

print("Pip packages:\n")
_internal.main(['list'])


blob_account_name = "dsan6000fall2024"
blob_container_name = "reddit-project"
wasbs_base_url = (
    f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/"
)
comments_path = "202306-202407/comments/"
submissions_path = "202306-202407/submissions/"
comments_single = "yyyy=2023/mm=11/comments_RC_2023-11.zst_9.parquet"
submissions_single = "yyyy=2023/mm=11/submissions_RS_2023-11.zst_36.parquet"
comments_single_df = spark.read.parquet(f"{wasbs_base_url}{comments_path}{comments_single}")
submissions_single_df = spark.read.parquet(f"{wasbs_base_url}{submissions_path}{submissions_single}")
comments_single_df.show()

data = spark.createDataFrame([['Peter is a goud person.']]).toDF('text')

data
