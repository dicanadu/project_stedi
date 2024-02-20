import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1708382101224 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1708382101224",
)

# Script generated for node customer trusted
customertrusted_node1708382138938 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1708382138938",
)

# Script generated for node Join
Join_node1708382205734 = Join.apply(
    frame1=customertrusted_node1708382138938,
    frame2=accelerometertrusted_node1708382101224,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1708382205734",
)

# Script generated for node Drop Fields
DropFields_node1708382248083 = DropFields.apply(
    frame=Join_node1708382205734,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1708382248083",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1708382351869 = DynamicFrame.fromDF(
    DropFields_node1708382248083.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1708382351869",
)

# Script generated for node customer curated
customercurated_node1708382413724 = glueContext.getSink(
    path="s3://diecadu-stedi-project/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customercurated_node1708382413724",
)
customercurated_node1708382413724.setCatalogInfo(
    catalogDatabase="project_stedi", catalogTableName="customer_curated"
)
customercurated_node1708382413724.setFormat("json")
customercurated_node1708382413724.writeFrame(DropDuplicates_node1708382351869)
job.commit()
