import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
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

# Script generated for node Ingest Customer Landing
IngestCustomerLanding_node1708363109977 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="customer_landing",
    transformation_ctx="IngestCustomerLanding_node1708363109977",
)

# Script generated for node Filter Customers with Research
SqlQuery73 = """
select * from myDataSource
WHERE shareWithResearchAsOfDate != 0;
"""
FilterCustomerswithResearch_node1708367051341 = sparkSqlQuery(
    glueContext,
    query=SqlQuery73,
    mapping={"myDataSource": IngestCustomerLanding_node1708363109977},
    transformation_ctx="FilterCustomerswithResearch_node1708367051341",
)

# Script generated for node Load Customers Trusted
LoadCustomersTrusted_node1708363335128 = glueContext.getSink(
    path="s3://diecadu-stedi-project/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="LoadCustomersTrusted_node1708363335128",
)
LoadCustomersTrusted_node1708363335128.setCatalogInfo(
    catalogDatabase="project_stedi", catalogTableName="customer_trusted"
)
LoadCustomersTrusted_node1708363335128.setFormat("json")
LoadCustomersTrusted_node1708363335128.writeFrame(
    FilterCustomerswithResearch_node1708367051341
)
job.commit()
