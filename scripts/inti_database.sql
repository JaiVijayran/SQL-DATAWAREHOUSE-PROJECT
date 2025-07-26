/*
Purpose: This script manages the creation of a DataWarehouse database in SQL Server.
It checks for the existence of a database named 'DataWareHouse', drops it if it exists, and recreates it. Additionally, it creates three schemas (bronze, silver, gold) within the new database to organize data at different processing stages.

Warning: This script includes a DROP DATABASE command that will permanently delete the existing 'DataWareHouse' database, including all its data, without recovery unless a backup exists. 
Ensure you have a backup of the database before executing this script, as data loss will occur if the database is dropped. Use with caution in production environments.
*/

USE master
GO

--So before creating the database we need to check that the database alredy exist in the server.
--DROP AND RECREATE THE 'DATAWAREHOUSE' DATABASE.

IF EXISTS (SELECT  1 FROM sys.databases WHERE name= 'DataWareHouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
	END;
	GO


--CREATE THE DATAWAREHOUSE DATABASE.
CREATE DATABASE DataWareHouse;
USE DataWareHouse;
GO
--CREATE THE SCHEMAS
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
 
