CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
  sensorReadingTime bigint,
  serialNumber string,
  distanceFromObject double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE'
)
LOCATION 's3://stedi-human-balance-ye/landing/step_trainer_landing/'
TBLPROPERTIES ('classification'='json');