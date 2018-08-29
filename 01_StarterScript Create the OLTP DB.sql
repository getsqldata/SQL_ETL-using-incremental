/************************************************************** 
 Data Warehouse Design LAB
(STEP 1) Create a lite version of the Northwind database
(STEP 2) Create a new Data Warehouse called DWNorthwindLite 
	  a) The DW should have 3 regular dimensions and 1 fact dimensions
	  b) The DW should have one fact table and 2 measures
 *************************************************************/

/***** (STEP 1) Create a lite version of the Northwind database *****/
-- Run this script to create the database you will need for the lab

If Exists(Select name from master.dbo.sysdatabases Where Name = 'NorthwindLite')
Begin
	USE [master];
	ALTER DATABASE [NorthwindLite] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [NorthwindLite];
End;
Go

Create Database NorthwindLite; 
Go

USE NorthwindLite;
Go

SELECT 
 [ProductID]
,[ProductName]
,[CategoryID]
INTO [Products]
FROM [Northwind].[dbo].[Products];
Go

SELECT 
 [CategoryID]
,[CategoryName]
INTO [Categories]
FROM [Northwind].[dbo].[Categories];
Go

SELECT 
 [CustomerID]
,[CompanyName]
,[ContactName]
,[Address]
,[City]
,[Country]
INTO [Customers]
FROM [Northwind].[dbo].[Customers];
Go

SELECT 
  [OrderID]
 ,[CustomerID]
 ,[OrderDate]
INTO [Orders]
FROM [Northwind].[dbo].[Orders];
Go

SELECT [OrderID]
,[ProductID]
,[UnitPrice]
,[Quantity]
INTO [OrderDetails]
FROM [Northwind].[dbo].[Order Details];
Go

-- Add Primary keys
ALTER TABLE dbo.Products ADD CONSTRAINT
	PK_Products PRIMARY KEY CLUSTERED (ProductID );

ALTER TABLE dbo.Categories ADD CONSTRAINT
	PK_Categories PRIMARY KEY CLUSTERED ( CategoryID );

ALTER TABLE dbo.Customers ADD CONSTRAINT
	PK_Customers PRIMARY KEY CLUSTERED ( CustomerID );

ALTER TABLE dbo.Orders ADD CONSTRAINT
	PK_Orders PRIMARY KEY CLUSTERED ( OrderId );

ALTER TABLE dbo.OrderDetails ADD CONSTRAINT
	PK_OrderDetais PRIMARY KEY CLUSTERED ( OrderId, ProductId );
Go

-- Add For
ALTER TABLE dbo.Products ADD CONSTRAINT
	FK_Products_Categories FOREIGN KEY(	CategoryID ) 
	REFERENCES dbo.Categories( CategoryID );

ALTER TABLE dbo.OrderDetails ADD CONSTRAINT
	FK_OrderDetails_Products FOREIGN KEY ( ProductID ) 
	REFERENCES dbo.Products	( ProductID ); 

ALTER TABLE dbo.OrderDetails ADD CONSTRAINT
	FK_OrderDetails_Orders FOREIGN KEY ( OrderID ) 
	REFERENCES dbo.Orders ( OrderID ) ;

ALTER TABLE dbo.Orders ADD CONSTRAINT
	FK_Orders_Customers FOREIGN KEY	( CustomerID ) 
	REFERENCES dbo.Customers (	CustomerID	);
Go
Select 'New database made with these objects:'
Select Name From SysObjects Where xtype in ('u', 'pk', 'f')
SELECT [TABLE_NAME]
      ,[COLUMN_NAME]
      ,[IS_NULLABLE]
      ,[DATA_TYPE]
      ,[CHARACTER_MAXIMUM_LENGTH]
      ,[NUMERIC_PRECISION]
      ,[NUMERIC_SCALE]
FROM [NorthwindLite].[INFORMATION_SCHEMA].[COLUMNS];
Go

/* (STEP 2) Create a new Data Warehouse DWNorthwindLite
	  a) The DW should have 3 regular dimensions and 1 fact dimensions
	  b) The DW should have one fact table and 2 measures
*/