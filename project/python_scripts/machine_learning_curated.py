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

# Script generated for node accelerometer trusted
accelerometertrusted_node1708388750378 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1708388750378",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1708388750963 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="steptrainertrusted_node1708388750963",
)

# Script generated for node Join SQL
SqlQuery0 = """
select *
from acc
join stp
on stp.sensorreadingtime = acc.timestamp
"""
JoinSQL_node1708388811661 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "acc": accelerometertrusted_node1708388750378,
        "stp": steptrainertrusted_node1708388750963,
    },
    transformation_ctx="JoinSQL_node1708388811661",
)

# Script generated for node Amazon S3
AmazonS3_node1708388909408 = glueContext.getSink(
    path="s3://diecadu-stedi-project/machine_learning/results/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1708388909408",
)
AmazonS3_node1708388909408.setCatalogInfo(
    catalogDatabase="project_stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1708388909408.setFormat("json")
AmazonS3_node1708388909408.writeFrame(JoinSQL_node1708388811661)
job.commit()
