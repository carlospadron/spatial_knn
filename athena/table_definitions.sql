CREATE EXTERNAL TABLE IF NOT EXISTS `gis`.`code_point_open_white_horse` (
    `postcode` string,
    `geom` binary,
    `wkt` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://giscarlos/codepoint_wh/'
TBLPROPERTIES ('classification' = 'csv');

CREATE EXTERNAL TABLE IF NOT EXISTS `gis`.`open_uprn_white_horse` (
    `uprn` bigint,
    `x_coordinate` double,
    `y_coordinate` double,
    `latitude` double,
    `longitude` double,
    `geom` binary,
    `wkt` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://giscarlos/uprn_wh/'
TBLPROPERTIES ('classification' = 'csv');
