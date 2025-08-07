/*
====================================================
Create Database and Schemas
====================================================
Script Purpose:
  The purpose of this script is to create a new database 'DataWarehouse', while first making sure that there is not already a database that exists with that same name.
  If the database does exist, then it is dropped and recreated.  The script also creates three, layered schemas: 'bronze', 'silver', and 'gold'.

WARNING:
  Running this script will drop the entire 'DateWarehouse' database if it exists.
*/

USE master;


-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DateWarehouse Set SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehosue' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
