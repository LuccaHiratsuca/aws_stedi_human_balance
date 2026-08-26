"""
Glue Job: accelerometer_landing_to_trusted
==========================================
Zone transition : Landing  ->  Trusted
Produces        : stedi.accelerometer_trusted   (expected 32025 rows)

Privacy rules
-------------
1. Inner join the accelerometer landing data against customer_trusted on email,
   so only readings belonging to a consenting customer survive.
2. STAND-OUT: drop any reading captured BEFORE the customer's consent took
   effect (`timestamp < sharewithresearchasofdate`). Consent is not applied
   retroactively -- if the customer later revokes it, every record we kept can
   still be shown to have been gathered while consent was in place.

   Without rule 2 this table would hold 40981 rows; with it, 32025.

Output columns are accelerometer columns ONLY -- no customer attribute leaks
into the Trusted zone.

Nodes
-----
  1. S3 bucket   -> accelerometer landing JSON
  2. Data Catalog-> stedi.customer_trusted
  3. SQL Query   -> consent join + consent-date cut-off
  4. S3 / Catalog-> accelerometer_trusted (schema auto-inferred & updated)
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
LANDING_PATH = "s3://stedi-lakehouse/accelerometer/landing/"
TRUSTED_PATH = "s3://stedi-lakehouse/accelerometer/trusted/"

# --- Node 1: Amazon S3 -- accelerometer landing zone -------------------------
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": [LANDING_PATH], "recurse": True},
    transformation_ctx="AccelerometerLanding_node1",
)

# --- Node 2: AWS Glue Data Catalog -- customer_trusted -----------------------
# Read the privacy table from the Catalog rather than raw S3: the Catalog node
# returns the complete, already-sanitised dataset.
CustomerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node2",
)

# --- Node 3: SQL Query -- privacy join + consent-date cut-off ----------------
# `timestamp` is a reserved word, hence the back-quotes.
# Both sides are Unix epoch milliseconds, so the comparison is a plain integer
# comparison -- no date parsing required.
SqlQuery0 = """
SELECT  a.user,
        a.`timestamp`,
        a.x,
        a.y,
        a.z
FROM        accelerometer_landing a
INNER JOIN  customer_trusted      c
        ON  a.user = c.email
WHERE   a.`timestamp` >= c.sharewithresearchasofdate
"""

AccelerometerTrusted_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1,
        "customer_trusted": CustomerTrusted_node2,
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

# --- Node 4: Amazon S3 -- accelerometer_trusted ------------------------------
AccelerometerTrustedSink_node4 = glueContext.getSink(
    path=TRUSTED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrustedSink_node4",
)
AccelerometerTrustedSink_node4.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE, catalogTableName="accelerometer_trusted"
)
AccelerometerTrustedSink_node4.setFormat("json")
AccelerometerTrustedSink_node4.writeFrame(AccelerometerTrusted_node3)

job.commit()
