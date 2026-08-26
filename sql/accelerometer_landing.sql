-- =============================================================================
-- Table:  stedi.accelerometer_landing
-- Zone:   Landing (raw, unfiltered)
-- Source: STEDI companion mobile app (phone accelerometer)
-- Rows:   81273
-- =============================================================================
-- Notes on typing:
--   * `timestamp` is a RESERVED WORD in Athena/Hive. The column must be
--     back-quoted in DDL and double-quoted in SELECTs. It holds a Unix epoch
--     value in MILLISECONDS -> BIGINT.
--   * x / y / z are signed accelerometer readings -> DOUBLE.
--   * `user` holds the customer email and is the join key back to
--     customer_trusted.email.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_landing` (
    `user`      string,
    `timestamp` bigint,
    `x`         double,
    `y`         double,
    `z`         double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'case.insensitive'      = 'TRUE',
    'ignore.malformed.json' = 'TRUE'
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
          OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lakehouse/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');


-- -----------------------------------------------------------------------------
-- Verification queries (screenshot: screenshots/accelerometer_landing.png)
-- -----------------------------------------------------------------------------

-- Expected: 81273
SELECT COUNT(*) AS accelerometer_landing_rows FROM stedi.accelerometer_landing;

-- Note the double quotes around the reserved word "timestamp"
SELECT "user", "timestamp", x, y, z
FROM stedi.accelerometer_landing
LIMIT 10;
