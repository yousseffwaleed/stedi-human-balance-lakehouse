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

# Read from Data Catalog landing table
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customer_landing"
)

# Filter only consented customers
query = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
"""
customer_trusted = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={"customer_landing": customer_landing},
    transformation_ctx="customer_trusted"
)

# Write to S3 + register catalog table
sink = glueContext.getSink(
    path="s3://stedi-human-balance-ye/trusted/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_trusted")
sink.setFormat("parquet")
sink.writeFrame(customer_trusted)

job.commit()