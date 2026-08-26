-- =============================================================================
-- Table:  stedi.step_trainer_landing
-- Zone:   Landing (raw, unfiltered)
-- Source: STEDI Step Trainer IoT motion sensor stream
-- Rows:   28680
-- =============================================================================
-- Notes on typing:
--   * sensorReadingTime is a Unix epoch value in MILLISECONDS -> BIGINT. It is
--     the join key against accelerometer_trusted."timestamp".
--   * distanceFromObject is a whole-number distance reading -> INT.
--   * serialNumber here is TRUSTWORTHY. The same field in customer_landing was
--     corrupted by the fulfillment-website defect, which is why the step
--     trainer feed has to be joined through customers_curated.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_landing` (
    `sensorreadingtime`  bigint,
    `serialnumber`       string,
    `distancefromobject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'case.insensitive'      = 'TRUE',
    'ignore.malformed.json' = 'TRUE'
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
          OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lakehouse/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');


-- -----------------------------------------------------------------------------
-- Verification queries (screenshot: screenshots/step_trainer_landing.png)
-- -----------------------------------------------------------------------------

-- Expected: 28680
SELECT COUNT(*) AS step_trainer_landing_rows FROM stedi.step_trainer_landing;

SELECT * FROM stedi.step_trainer_landing LIMIT 10;
