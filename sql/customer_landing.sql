CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  serialnumber string,
  sharewithpublicasofdate bigint,
  birthday string,
  registrationdate bigint,
  sharewithresearchasofdate bigint,
  customername string,
  email string,
  lastupdatedate bigint,
  phone string,
  sharewithfriendsasofdate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE'
)
LOCATION 's3://stedi-human-balance-ye/landing/customer_landing/'
TBLPROPERTIES ('classification'='json');