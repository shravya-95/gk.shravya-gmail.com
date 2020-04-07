--*************************************
--CREATE EXTERNAL TABLES on Raw Train Data
--*************************************

--command to run
--
--${var:database_name}
--Create Database

SET VAR:database_name=zeros_and_ones_raw;

Create Database IF NOT EXISTS ${var:database_name}
COMMENT 'Raw sales data imported from salesdb';


--Create External sales Table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.sales (
	orderid int,
	SalesPersonID int,
	CustomerID int,
	ProductID int, 
	Quantity int, 
	Date timestamp)
COMMENT 'Sales table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/salesdb/Sales2/'
TBLPROPERTIES ("skip.header.line.count"="1");




--Create External Products Table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.products (
	productid int,
	Name varchar,
	Price int)
COMMENT 'Products Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/salesdb/Products/'
TBLPROPERTIES ("skip.header.line.count"="1");

---Create external employees table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.employees (
	EmployeeID int,
	FirstName varchar,
	MiddleInitial char,
	LastName varchar,
	Region varchar)
COMMENT 'employees Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/salesdb/Employees2/'
TBLPROPERTIES ("skip.header.line.count"="1");

---Create external employees table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.customers (
	CustomerID int,
	FirstName varchar,
	MiddleInitial varchar,
	LastName varchar)
COMMENT 'employees Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/salesdb/Customers2/'
TBLPROPERTIES ("skip.header.line.count"="1");


invalidate metadata;
compute stats ${var:database_name}.sales;
compute stats ${var:database_name}.products;
compute stats ${var:database_name}.employees;
compute stats ${var:database_name}.customers;