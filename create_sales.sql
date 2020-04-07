--*************************************
--CREATE Parquet TABLES on Train Data
--*************************************


SET VAR:database_name=chad_trains;

SET VAR:source_database=chad_trains_raw;

Create Database IF NOT EXISTS ${var:database_name}
COMMENT 'Parquet trains data imported from trains database';

--Create Parquet Cars Table

CREATE TABLE IF NOT EXISTS ${var:database_name}.cars
COMMENT 'Parquet cars table'
STORED AS Parquet
AS
SELECT * from ${var:source_database}.cars;


--Create Parquet trains Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.trains
COMMENT 'Parquet trains table'
STORED AS Parquet
AS
SELECT *
from ${var:source_database}.trains;

invalidate metadata;
compute stats ${var:database_name}.cars;
compute stats ${var:database_name}.trains;