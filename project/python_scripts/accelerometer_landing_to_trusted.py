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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1708370281537 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometerlanding_node1708370281537",
)

# Script generated for node Customer trusted
Customertrusted_node1708370283783 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="customer_trusted",
    transformation_ctx="Customertrusted_node1708370283783",
)

# Script generated for node Join consent
Joinconsent_node1708370426137 = Join.apply(
    frame1=Accelerometerlanding_node1708370281537,
    frame2=Customertrusted_node1708370283783,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Joinconsent_node1708370426137",
)

# Script generated for node Drop columns
SqlQuery0 = """
SELECT user, timestamp, x, y, z from myDataSource;
"""
Dropcolumns_node1708371550300 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Joinconsent_node1708370426137},
    transformation_ctx="Dropcolumns_node1708371550300",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1708370600539 = glueContext.getSink(
    path="s3://diecadu-stedi-project/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="Accelerometertrusted_node1708370600539",
)
Accelerometertrusted_node1708370600539.setCatalogInfo(
    catalogDatabase="project_stedi", catalogTableName="accelerometer_trusted"
)
Accelerometertrusted_node1708370600539.setFormat("json")
Accelerometertrusted_node1708370600539.writeFrame(Dropcolumns_node1708371550300)
job.commit()
