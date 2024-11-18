import sparknlp
import pyspark
from sparknlp.annotator import *
from sparknlp.pretrained import *
from pyspark.sql import SparkSession

print("Spark NLP version: ", sparknlp.version())

spark = SparkSession.builder \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1") \
    .getOrCreate()

print(f"Apache Spark version: {spark.version}")

spark.sparkContext.getConf().getAll()

import sys
print(f"Python version: {sys.version}")

from pip import _internal
print(f"pip version: {_internal.main(['show', 'pip'])}")

print("Pip packages:\n")
_internal.main(['list'])

print("Printing Spark COnfiguration")
print(spark.sparkContext.getConf().getAll())

print("Data Access Test")

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

print("Data Access Test Passed")

print("Begin Spark NLP Test")

data = spark.createDataFrame([['Peter is a goud person.']]).toDF('text')

data

print("Calling pretrained pipeline")

pipeline = PretrainedPipeline('explain_document_ml')

result = pipeline.transform(data)

result.show()

from sparknlp.functions import *

from sparknlp.annotation import Annotation

def my_annoation_map_function(annotations):
    return list(map(lambda a: Annotation(
        'my_own_type',
        a.begin,
        a.end,
        a.result,
        {'my_key': 'custom_annotation_data'},
        []), annotations))

result.select(
    map_annotations(my_annoation_map_function, Annotation.arrayType())('token')
).toDF("my output").show(truncate=False)

explode_annotations_col(result, 'lemmas.result', 'exploded').select('exploded').show()

