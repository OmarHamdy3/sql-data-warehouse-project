/*
===============================================================================
 Script Name   : Create_DataWarehouse.sql
 Purpose       : Creates a new SQL Server database called 'DataWarehouse'
                 using Medallion Architecture (Bronze, Silver, Gold schemas).
                 Useful for setting up a data warehouse environment from scratch.

 ⚠️ WARNING:
 This script will DELETE the existing 'DataWarehouse' database if it exists.
 All data will be permanently lost. DO NOT run in production environments
 without a backup and proper validation.
===============================================================================
*/

-- Switch to the 'master' database to perform administrative tasks like dropping/creating other databases
USE master;

-- Check if a database named 'DataWarehouse' already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
    -- If it exists, set it to SINGLE_USER mode and roll back any open transactions
    -- This is necessary to safely drop the database if it is in use
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    
    -- Drop the existing 'DataWarehouse' database
    DROP DATABASE DataWarehouse;
END;
GO

-- Create a new database named 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

-- Switch to the newly created 'DataWarehouse' database
USE DataWarehouse;
GO 

-- Create a schema named 'bronze' to store raw, unprocessed data (e.g., directly loaded from source systems)
CREATE SCHEMA bronze;
GO

-- Create a schema named 'silver' to store cleaned, transformed, and standardized data
CREATE SCHEMA silver;
GO

-- Create a schema named 'gold' to store business-ready data (usually in a star schema format for analytics)
CREATE SCHEMA gold;
GO
