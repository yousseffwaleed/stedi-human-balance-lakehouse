CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
  timeStamp bigint,
  user string,
  x double,
  y double,
  z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE'
)
LOCATION 's3://stedi-human-balance-ye/landing/accelerometer_landing/'
TBLPROPERTIES ('classification'='json');