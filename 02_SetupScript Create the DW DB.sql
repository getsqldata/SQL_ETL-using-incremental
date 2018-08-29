/************************************************************** 
 Create the Data Warehouse 
*************************************************************/

--****************** [DWNorthwindLite_withSCD] *********************--
-- This file will drop and create the [DWNorthwindLite_withSCD]
-- database, with all its objects. 
--****************** Instructors Version ***************************--

USE [master]
GO
If Exists (Select * from Sysdatabases Where Name = 'DWNorthwindLite_withSCD')
	Begin 
		ALTER DATABASE [DWNorthwindLite_withSCD] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
		DROP DATABASE [DWNorthwindLite_withSCD]
	End
GO
Create Database [DWNorthwindLite_withSCD]
Go

--********************************************************************--
-- Create the Tables
--********************************************************************--
USE [DWNorthwindLite_withSCD]
Go

/****** [dbo].[DimProducts] ******/
CREATE TABLE DWNorthwindLite_withSCD.dbo.DimProducts(
	 ProductKey int	IDENTITY   		   NOT NULL
	,ProductID int			   		   NOT NULL
	,ProductName nVarchar(100) 		   NOT NULL
	,ProductCategoryID int	   		   NOT NULL
	,ProductCategoryName nVarchar(100) NOT NULL 
	,StartDate int			   		   NOT NULL
	,EndDate int			  		   NULL
	,IsCurrent char(3)		  		   NOT NULL
	CONSTRAINT PK_DimProducts PRIMARY KEY (ProductKey)
)
Go

/****** [dbo].[DimCustomers] ******/
CREATE TABLE DWNorthwindLite_withSCD.dbo.DimCustomers(
	 CustomerKey int IDENTITY	   NOT NULL
	,CustomerID nchar(5)		   NOT NULL
	,CustomerName nVarchar(100)	   NOT NULL
	,CustomerCity nVarchar(100)	   NOT NULL
	,CustomerCountry nVarchar(100) NOT NULL
	,StartDate int				   NOT NULL
	,EndDate int				   NULL
	,IsCurrent char(3)			   NOT NULL
	CONSTRAINT PK_DimCustomers PRIMARY KEY (CustomerKey)
)
Go

/****** [dbo].[DimDates] ******/
CREATE TABLE DWNorthwindLite_withSCD.dbo.DimDates(
	 DateKey int			   NOT NULL
	,USADateName nVarchar(100) NOT NULL
	,MonthKey int			   NOT NULL
	,MonthName nVarchar(100)   NOT NULL
	,QuarterKey int			   NOT NULL
	,QuarterName nVarchar(100) NOT NULL
	,YearKey int			   NOT NULL
	,YearName nVarchar(100)	   NOT NULL
	CONSTRAINT PK_DimDates PRIMARY KEY (DateKey)
)
Go

/****** [dbo].[FactOrders] ******/
CREATE TABLE DWNorthwindLite_withSCD.dbo.FactOrders(
	 OrderID int				 NOT NULL
	,CustomerKey int			 NOT NULL
	,OrderDateKey int			 NOT NULL
	,ProductKey int				 NOT NULL
	,ActualOrderUnitPrice money	 NOT NULL
	,ActualOrderQuantity int	 NOT NULL
	CONSTRAINT PK_FactOrders PRIMARY KEY (OrderID,CustomerKey,OrderDateKey,ProductKey)
)
Go

--********************************************************************--
-- Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--
ALTER TABLE DWNorthwindLite_withSCD.dbo.FactOrders
  ADD CONSTRAINT fkFactOrdersToDimProducts
  FOREIGN KEY (ProductKey) REFERENCES DimProducts(ProductKey)

ALTER TABLE DWNorthwindLite_withSCD.dbo.FactOrders
  ADD CONSTRAINT fkFactOrdersToDimCustomers
  FOREIGN KEY (CustomerKey) REFERENCES DimCustomers(CustomerKey)

ALTER TABLE DWNorthwindLite_withSCD.dbo.FactOrders
  ADD CONSTRAINT fkFactOrdersToDimDates 
  FOREIGN KEY (OrderDateKey) REFERENCES DimDates(DateKey)

--********************************************************************--
-- Review the results of this script
--********************************************************************--
Select 'Database Created'
Select Name, xType, CrDate from SysObjects 
Where xType in ('u', 'PK', 'F')
Order By xType desc, Name