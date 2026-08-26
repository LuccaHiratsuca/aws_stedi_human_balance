"""
Glue Job: customer_landing_to_trusted
=====================================
Zone transition : Landing  ->  Trusted
Produces        : stedi.customer_trusted   (expected 482 rows)

Privacy rule
------------
Only customers who explicitly consented to share their data for research are
promoted to the Trusted zone. In the raw feed a non-consenting customer simply
OMITS the `shareWithResearchAsOfDate` key, which Spark surfaces as NULL, so the
filter is `sharewithresearchasofdate IS NOT NULL`.

Nodes
-----
  1. S3 bucket            -> customer landing JSON
  2. SQL Query            -> drop rows with no research consent on record
  3. S3 bucket / Catalog  -> customer_trusted (schema auto-inferred & updated)
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
LANDING_PATH = "s3://stedi-lakehouse/customer/landing/"
TRUSTED_PATH = "s3://stedi-lakehouse/customer/trusted/"

# --- Node 1: Amazon S3 -- customer landing zone ------------------------------
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": [LANDING_PATH], "recurse": True},
    transformation_ctx="CustomerLanding_node1",
)

# --- Node 2: SQL Query -- filter for research consent ------------------------
# A Transform-SQL node is used instead of a Filter node: it gives deterministic
# output and makes the privacy rule reviewable as plain SQL.
SqlQuery0 = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
"""

CustomerTrusted_node2 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": CustomerLanding_node1},
    transformation_ctx="CustomerTrusted_node2",
)

# --- Node 3: Amazon S3 -- customer_trusted -----------------------------------
# enableUpdateCatalog + UPDATE_IN_DATABASE is the "Create a table in the Data
# Catalog and, on subsequent runs, update the schema and add new partitions"
# option, i.e. the schema is dynamically inferred and kept in sync.
CustomerTrustedSink_node3 = glueContext.getSink(
    path=TRUSTED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrustedSink_node3",
)
CustomerTrustedSink_node3.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE, catalogTableName="customer_trusted"
)
CustomerTrustedSink_node3.setFormat("json")
CustomerTrustedSink_node3.writeFrame(CustomerTrusted_node2)

job.commit()
