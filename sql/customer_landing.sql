-- =============================================================================
-- Table:  stedi.customer_landing
-- Zone:   Landing (raw, unfiltered)
-- Source: STEDI fulfillment website + mobile app registration
-- Rows:   956
-- =============================================================================
-- Notes on typing:
--   * The *AsOfDate / registrationDate / lastUpdateDate fields are Unix epoch
--     timestamps in MILLISECONDS, so they are typed as BIGINT (not string).
--     Customers who never consented to research simply OMIT the
--     shareWithResearchAsOfDate key, which the SerDe surfaces as NULL.
--   * phone stays a string: it is an identifier, not a number (leading zeros,
--     no arithmetic meaning).
--   * birthDay stays a string: the raw feed contains out-of-range values such
--     as "1323-01-01" that would break a strict DATE cast.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
    `customername`              string,
    `email`                     string,
    `phone`                     string,
    `birthday`                  string,
    `serialnumber`              string,
    `registrationdate`          bigint,
    `lastupdatedate`            bigint,
    `sharewithresearchasofdate` bigint,
    `sharewithpublicasofdate`   bigint,
    `sharewithfriendsasofdate`  bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'case.insensitive'     = 'TRUE',
    'ignore.malformed.json' = 'TRUE'
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
          OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lakehouse/customer/landing/'
TBLPROPERTIES ('classification' = 'json');


-- -----------------------------------------------------------------------------
-- Verification queries (screenshot: screenshots/customer_landing.png)
-- -----------------------------------------------------------------------------

-- Expected: 956
SELECT COUNT(*) AS customer_landing_rows FROM stedi.customer_landing;

-- Expected: 474 rows with no research consent on record (blank / NULL)
SELECT COUNT(*) AS blank_share_with_research
FROM stedi.customer_landing
WHERE sharewithresearchasofdate IS NULL;

SELECT * FROM stedi.customer_landing LIMIT 10;
