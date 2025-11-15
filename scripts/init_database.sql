-- Create Database: 'DataWarehouse'
use master;
GO

-- drop and recreate the DataWarehouse database
IF EXISTS(SELECT 1 from sys.databases WHERE name = 'DataWarehouse')
BEGIN
    alter DATABASE DataWarehouse set single_user with ROLLBACK IMMEDIATE;
    drop DATABASE DataWarehouse;
END;
GO

create database DataWarehouse;
go


use DataWarehouse;
go
-- Create Schemas
create schema bronze;
go
create schema silver;
go 
create schema gold;


