import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    spark = glueContext.spark_session
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)

query = """
SELECT DISTINCT c.*
FROM c
INNER JOIN a
ON c.email = a.user
"""
customer_curated = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={"c": customer_trusted, "a": accelerometer_trusted},
    transformation_ctx="customer_curated"
)

sink = glueContext.getSink(
    path="s3://stedi-human-balance-ye/curated/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_curated")
sink.setFormat("parquet")
sink.writeFrame(customer_curated)

job.commit()