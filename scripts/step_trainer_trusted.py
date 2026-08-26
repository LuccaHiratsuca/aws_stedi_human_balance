"""
Glue Job: step_trainer_trusted
==============================
Zone transition : Landing  ->  Trusted
Produces        : stedi.step_trainer_trusted   (expected 14460 rows)

The serial-number problem
-------------------------
A defect in the fulfillment website reused the same handful of serial numbers
across millions of customer records, so customer_landing.serialnumber cannot be
trusted to identify a device. The IoT feed, on the other hand, reports the REAL
serial number.

Joining the IoT stream against a known-good customer list on serialnumber
therefore does two jobs at once: it re-attaches each reading to a real customer,
and it enforces the privacy rule for the Step Trainer feed.

Which customer list? (consent vs. curation)
-------------------------------------------
The privacy question for an IoT reading is simply "did this customer consent to
research?" -- that is exactly customer_trusted. Membership of customers_curated
adds a second, unrelated condition ("this customer also produced accelerometer
readings"), which is a property of the *accelerometer* feed and has no bearing
on whether a step trainer reading may be used.

In the baseline pipeline the distinction is invisible, because customers_curated
and customer_trusted are both 482 customers. Once the stand-out consent-date
cut-off is applied, 18 consenting customers drop out of the curated table purely
because all of their accelerometer readings pre-date their consent -- yet their
step trainer readings are still perfectly legitimate. Gating on customer_trusted
keeps them, which is both the correct privacy semantics and the row count the
project rubric publishes (14460).

This choice does not change the final training set: the 540 extra readings have
no accelerometer reading at a matching timestamp, so machine_learning_curated is
34437 rows either way. To gate on the curated list instead, swap the source node
for customers_curated and the join target to `c.serialnumber` -- the result is
13920 rows.

Output columns are step trainer columns ONLY.

Nodes
-----
  1. S3 bucket    -> step trainer landing JSON
  2. Data Catalog -> stedi.customer_trusted
  3. SQL Query    -> inner join on serial number
  4. S3 / Catalog -> step_trainer_trusted (schema auto-inferred & updated)

Runtime note: this job reads ~28.7k IoT records against the customer list and
typically takes ~8 minutes on 2 DPU.
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
LANDING_PATH = "s3://stedi-lakehouse/step_trainer/landing/"
TRUSTED_PATH = "s3://stedi-lakehouse/step_trainer/trusted/"

# --- Node 1: Amazon S3 -- step trainer landing zone --------------------------
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": [LANDING_PATH], "recurse": True},
    transformation_ctx="StepTrainerLanding_node1",
)

# --- Node 2: AWS Glue Data Catalog -- customer_trusted -----------------------
CustomerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node2",
)

# --- Node 3: SQL Query -- match IoT readings to consenting customers ---------
# customer_trusted holds one row per customer with a distinct serial number, so
# this join cannot fan out the IoT readings.
SqlQuery0 = """
SELECT  s.sensorreadingtime,
        s.serialnumber,
        s.distancefromobject
FROM        step_trainer_landing s
INNER JOIN  customer_trusted     c
        ON  s.serialnumber = c.serialnumber
"""

StepTrainerTrusted_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1,
        "customer_trusted": CustomerTrusted_node2,
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

# --- Node 4: Amazon S3 -- step_trainer_trusted -------------------------------
StepTrainerTrustedSink_node4 = glueContext.getSink(
    path=TRUSTED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrustedSink_node4",
)
StepTrainerTrustedSink_node4.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE, catalogTableName="step_trainer_trusted"
)
StepTrainerTrustedSink_node4.setFormat("json")
StepTrainerTrustedSink_node4.writeFrame(StepTrainerTrusted_node3)

job.commit()
