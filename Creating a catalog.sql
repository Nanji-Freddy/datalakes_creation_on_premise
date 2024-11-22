
DROP CATALOG IF EXISTS iceberg_catalog;

CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="hadoop",
    "iceberg.catalog.warehouse"="s3a://iceberg-demo-bucket/warehouse",-- same with the one in the Spark config
    "aws.s3.endpoint"="http://minio:9000",
    "aws.s3.access_key"="datalake",
    "aws.s3.secret_key"="datalakekey",
    "aws.s3.enable_ssl" = "false",
 "aws.s3.enable_path_style_access" = "true"
);



SHOW CATALOGS;


SET CATALOG iceberg_catalog;

SHOW DATABASES FROM iceberg_catalog;

USE iceberg_demo;

SHOW tables;

SELECT * FROM car_ecommerece

SELECT car_name,country FROM  inconsistent_ad_campaign_data_with_cars






