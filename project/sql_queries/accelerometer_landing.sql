CREATE EXTERNAL TABLE IF NOT EXISTS `project_stedi`.`accelerometer_landing` (
  `user` string,
  `timestamp` bigint,
  `x` bigint,
  `y` bigint,
  `z` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://diecadu-stedi-project/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
