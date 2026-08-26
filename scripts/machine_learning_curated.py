"""
Glue Job: machine_learning_curated
==================================
Zone transition : Trusted  ->  Curated
Produces        : stedi.machine_learning_curated   (expected 34437 rows)

Purpose
-------
The training set for the step-detection model. Each Step Trainer distance
reading is paired with the accelerometer reading captured at the very same
instant, giving the model both the motion signal (x/y/z) and the ground-truth
distance in a single row.

Both sides are already privacy-filtered upstream, so every row here belongs to
a customer who consented to research and was consenting at capture time.

STAND-OUT: anonymisation
------------------------
`accelerometer_trusted.user` (the customer's email address) is deliberately
NOT carried through. The training set therefore contains no direct personal
identifier, which keeps it outside the scope of a GDPR erasure request -- if a
customer asks for their PII to be deleted, this table needs no remediation.

`serialnumber` is retained: it identifies a *device*, not a person, and the ML
team needs it to group readings per unit. It is only re-linkable to a customer
via the curated zone, which is where an erasure request would be actioned.

Nodes
-----
  1. Data Catalog -> stedi.step_trainer_trusted
  2. Data Catalog -> stedi.accelerometer_trusted
  3. SQL Query    -> inner join on reading time, PII dropped
  4. S3 / Catalog -> machine_learning_curated (schema auto-inferred & updated)
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
CURATED_PATH = "s3://stedi-lakehouse/machine_learning/curated/"

# --- Node 1: AWS Glue Data Catalog -- step_trainer_trusted -------------------
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# --- Node 2: AWS Glue Data Catalog -- accelerometer_trusted ------------------
AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# --- Node 3: SQL Query -- align the two sensors on a shared instant ----------
# `timestamp` is a reserved word, hence the back-quotes.
# a.user is intentionally omitted -- see the anonymisation note above.
SqlQuery0 = """
SELECT  s.sensorreadingtime,
        s.serialnumber,
        s.distancefromobject,
        a.x,
        a.y,
        a.z
FROM        step_trainer_trusted  s
INNER JOIN  accelerometer_trusted a
        ON  s.sensorreadingtime = a.`timestamp`
"""

MachineLearningCurated_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1,
        "accelerometer_trusted": AccelerometerTrusted_node2,
    },
    transformation_ctx="MachineLearningCurated_node3",
)

# --- Node 4: Amazon S3 -- machine_learning_curated ---------------------------
MachineLearningCuratedSink_node4 = glueContext.getSink(
    path=CURATED_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCuratedSink_node4",
)
MachineLearningCuratedSink_node4.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE, catalogTableName="machine_learning_curated"
)
MachineLearningCuratedSink_node4.setFormat("json")
MachineLearningCuratedSink_node4.writeFrame(MachineLearningCurated_node3)

job.commit()
