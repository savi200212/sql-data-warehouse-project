/*
=======================================================================
Create Database and Schemas
=======================================================================
Script Purpose:
   This script creates a new database named 'DataWarehouse' after checking if it already exists.
   If the database exists,it is dropped and recreated.Additionally,the script sets up three schemas
   within the database: 'bronze','silver','gold'

WARNING:
  Running this script willl drop the entire 'DataWarehouse' database if it exists.
  All data in the database will be permanently deleted.Proceed with caution
  and ensure you have proper backups before running this script.
*/


---Create Database 'DataWarehouse'-----
USE master;
Create database DataWarehouse;

GO
----Create Schemas-----

create schema bronze;
GO
create schema silver;
GO
create schema gold;
