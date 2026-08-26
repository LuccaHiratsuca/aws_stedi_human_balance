"""
Glue Job: customer_trusted_to_curated
=====================================
Zone transition : Trusted  ->  Curated
Produces        : stedi.customers_curated   (expected 464 rows)

Business rule
-------------
A customer belongs in the Curated zone only if they BOTH consented to research
(already guaranteed by customer_trusted) AND actually produced accelerometer
data. The inner join against accelerometer_trusted enforces the second half.

Because accelerometer_trusted already had the consent-date cut-off applied, a
customer whose only readings pre-date their consent drops out here as well --
that is why this table holds 464 rows rather than the full 482 of
customer_trusted.

SELECT DISTINCT keeps the grain at one row per customer: without it the join
would fan a customer out across every accelerometer reading they produced.

Nodes
-----
  1. Data Catalog -> stedi.customer_trusted
  2. Data Catalog -> stedi.accelerometer_trusted
  3. SQL Query    -> inner join on email, customer columns only, de-duplicated
  4. S3 / Catalog -> customers_curated (schema auto-inferred & updated)
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

GLUE_DATABASE = "stedi"
CURATED_PATH = "s3://stedi-lakehouse/customer/curated/"

# --- Node 1: AWS Glue Data Catalog -- customer_trusted -----------------------
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# --- Node 2: AWS Glue Data Catalog -- accelerometer_trusted ------------------
AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# --- Node 3: SQL Query -- customers that have accelerometer data -------------
SqlQuery0 = """
SELECT DISTINCT
        c.customername,
        c.email,
        c.phone,
        c.birthday,
        c.serialnumber,
        c.registrationdate,
        c.lastupdatedate,
        c.sharewithresearchasofdate,
        c.sharewithpublicasofdate,
        c.sharewithfriendsasofdate
FROM        customer_trusted      c
INNER JOIN  accelerometer_trusted a
        ON  c.email = a.user
"""

CustomersCurated_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": CustomerTrusted_node1,
        "accelerometer_trusted": AccelerometerTrusted_node2,
    },
    transformation_ctx="CustomersCurated_node3",
)

# --- Node 4: Amazon S3 -- customers_curated ----------------------------------
CustomersCuratedSink_node4 = glueContext.getSink(
    path=CURATED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="CustomersCuratedSink_node4",
)
CustomersCuratedSink_node4.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE, catalogTableName="customers_curated"
)
CustomersCuratedSink_node4.setFormat("json")
CustomersCuratedSink_node4.writeFrame(CustomersCurated_node3)

job.commit()
