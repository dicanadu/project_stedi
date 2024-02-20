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

# Script generated for node customer curated
customercurated_node1708383633796 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1708383633796",
)

# Script generated for node step trainer landing
steptrainerlanding_node1708383635603 = glueContext.create_dynamic_frame.from_catalog(
    database="project_stedi",
    table_name="step_trainer_landing",
    transformation_ctx="steptrainerlanding_node1708383635603",
)

# Script generated for node Join and select columns
SqlQuery0 = """
SELECT s.sensorreadingtime, s.serialnumber, s.distancefromobject
from c
join s
on c.serialnumber=s.serialnumber;
"""
Joinandselectcolumns_node1708387003429 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "c": customercurated_node1708383633796,
        "s": steptrainerlanding_node1708383635603,
    },
    transformation_ctx="Joinandselectcolumns_node1708387003429",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1708384093069 = glueContext.getSink(
    path="s3://diecadu-stedi-project/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="steptrainertrusted_node1708384093069",
)
steptrainertrusted_node1708384093069.setCatalogInfo(
    catalogDatabase="project_stedi", catalogTableName="step_trainer_trusted"
)
steptrainertrusted_node1708384093069.setFormat("json")
steptrainertrusted_node1708384093069.writeFrame(Joinandselectcolumns_node1708387003429)
job.commit()
