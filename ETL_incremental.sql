--*************************************************************************--
-- Title: Assignment03
-- Author: <ByungsuJung>
-- Desc: This file tests you knowlege on how to create a Incremental ETL process with SQL code
-- Change Log: When,Who,What
-- 2018-01-17,<ByungsuJung>,Created File

-- Instructions: 
-- (STEP 1) Create a lite version of the Northwind database by running the provided code.
-- (STEP 2) Create a new Data Warehouse called DWNorthwindLite_withSCD based on the NorthwindLite DB.
--          The DW should have three dimension tables (for Customers, Products, and Dates) and one fact table.
-- (STEP 3) Fill the DW by creating an ETL Script
--**************************************************************************--
USE [DWNorthwindLite_withSCD];
go
SET NoCount ON;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimProducts')
   Drop View vETLDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncDimProducts')
   Drop Procedure pETLSyncDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimCustomers')
   Drop View vETLDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncDimCustomers')
   Drop Procedure pETLSyncDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go
	If Exists(Select * from Sys.objects where Name = 'vETLFactOrders')
   Drop View vETLFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncFactOrders')
   Drop Procedure pETLSyncFactOrders;

--********************************************************************--
-- A) NOT NEEDED FOR INCREMENTAL LOADING: 
 --   Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
--********************************************************************--

--********************************************************************--
-- B) Synchronize the Tables
--********************************************************************--

/****** [dbo].[DimProducts] ******/
go 
Create View vETLDimProducts
/* Author: <ByungsuJung>
** Desc: Extracts and transforms data for DimProducts
** Change Log: When,Who,What
** 20189-01-17,<ByungsuJung>,Created Sproc.
*/
As
  SELECT
    [ProductID] = p.ProductID
   ,[ProductName] = CAST(p.ProductName as nVarchar(100))
   ,[ProductCategoryID] = p.CategoryID
   ,[ProductCategoryName] = CAST(c.CategoryName as nVarchar(100))
  FROM [NorthwindLite].dbo.Categories as c
  INNER JOIN [NorthwindLite].dbo.Products as p
  ON c.CategoryID = p.CategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

go
Create Procedure pETLSyncDimProducts
/* Author: <ByungsuJung>
** Desc: Updates data in DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 20189-01-17,<ByungsuJung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
	With ChangedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
			Except
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
    )UPDATE [DWNorthwindLite_withSCD].dbo.DimProducts 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE ProductID IN (Select ProductID From ChangedProducts)
    ;
    -- 2)For INSERT or UPDATES: Add new rows to the table
	With AddedORChangedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
			Except
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
		)INSERT INTO [DWNorthwindLite_withSCD].dbo.DimProducts
      ([ProductID],[ProductName],[ProductCategoryID],[ProductCategoryName],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [ProductID]
       ,[ProductName]
       ,[ProductCategoryID]
       ,[ProductCategoryName]
       ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
       ,[EndDate] = Null
       ,[IsCurrent] = 1
      FROM vETLDimProducts
      WHERE ProductID IN (Select ProductID From AddedORChangedProducts)
    ;
    -- 3) For Delete: Change the IsCurrent status to zero
    With DeletedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- We do not care about row already marked zero!
 			Except            			
      Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
   	)UPDATE [DWNorthwindLite_withSCD].dbo.DimProducts 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE ProductID IN (Select ProductID From DeletedProducts)
   ;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimProducts;
 Print @Status;
 Select * From DimProducts Order By ProductID
*/


/****** [dbo].[DimCustomers] ******/
go 
Create View vETLDimCustomers
/* Author: <ByungsuJung>
** Desc: Extracts and transforms data for DimCustomers
** Change Log: When,Who,What
** 20189-01-17,<ByungsuJung>,Created Sproc.
*/
As
  SELECT
	[CustomerID] = c.CustomerID, 
	[CustomerName] = CAST(c.CompanyName as nVarchar(100)), 
	[CustomerCity] = CAST(c.City as nVarchar(100)),
	[CustomerCountry] = CAST(c.Country as nVarchar(100))
  From NorthwindLite.dbo.Customers as c  
go
/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLSyncDimCustomers
/* Author: <ByungsuJung>
** Desc: Inserts data into DimCustomers
** Change Log: When,Who,What
** 20189-01-17,<ByungsuJung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
		With ChangedCustomers 
		As(
			Select CustomerID, CustomerName, CustomerCity, CustomerCountry From vETLDimCustomers
			Except
			Select CustomerID, CustomerName, CustomerCity, CustomerCountry From DimCustomers
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
    )UPDATE [DWNorthwindLite_withSCD].dbo.DimCustomers 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE CustomerID IN (Select CustomerID From ChangedCustomers)
    ;
    -- 2) For INSERT or UPDATES: Add new rows to the table
		With AddedORChangedCustomers 
		As(
			Select CustomerID, CustomerName, CustomerCity, CustomerCountry  From vETLDimCustomers
			Except
			Select CustomerID, CustomerName, CustomerCity, CustomerCountry  From DimCustomers
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
		)INSERT INTO [DWNorthwindLite_withSCD].dbo.DimCustomers
      ([CustomerID],[CustomerName],[CustomerCity],[CustomerCountry],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [CustomerID]
       ,[CustomerName]
       ,[CustomerCity]
	   ,[CustomerCountry]
       ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
       ,[EndDate] = Null
       ,[IsCurrent] = 1
      FROM vETLDimCustomers
      WHERE CustomerID IN (Select CustomerID From AddedORChangedCustomers)
    ;
    -- 3) For Delete: Change the IsCurrent status to zero
     With DeletedCustomers 
		As(
			Select CustomerID, CustomerName, CustomerCity, CustomerCountry From DimCustomers
       Where IsCurrent = 1 -- We do not care about row already marked zero!
 			Except            			
      Select CustomerID, CustomerName, CustomerCity, CustomerCountry From vETLDimCustomers
   	)UPDATE [DWNorthwindLite_withSCD].dbo.DimCustomers 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE CustomerID IN (Select CustomerID From DeletedCustomers)
   ;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimCustomers;
 Print @Status;
*/
go

/****** [dbo].[DimDates] ******/
Create Procedure pETLFillDimDates
/* Author: <ByungsuJung>
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 20189-01-17,<ByungsuJung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	  Alter Table DwNorthwindLite_withSCD.dbo.FactOrders
		Drop Constraint fkFactOrdersToDimDates;
      Delete From DimDates; -- Clears table data with the need for dropping FKs
      Declare @StartDate datetime = '01/01/1990'
      Declare @EndDate datetime = '12/31/1999' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row into the date dimension table for this date
       Insert Into DimDates 
       ( [DateKey], [USADateName], [MonthKey], [MonthName], [QuarterKey], [QuarterName], [YearKey], [YearName] )
       Values ( 
         Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [DateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthKey]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Cast(DateName(YYYY,@DateInProcess) + '0' + (DateName(quarter, @DateInProcess) ) as int)  -- [QuarterKey]
        ,'Q' + DateName(quarter, @DateInProcess) + ' - ' + Cast( Year(@DateInProcess) as nVarchar(50) ) -- [QuarterName] 
        ,Year(@DateInProcess) -- [YearKey] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End
	ALTER TABLE DWNorthwindLite_withSCD.dbo.FactOrders
		ADD CONSTRAINT fkFactOrdersToDimDates 
		FOREIGN KEY (OrderDateKey) REFERENCES DimDates(DateKey)
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
*/
go

/****** [dbo].[FactOrders] ******/
go 
Create View vETLFactOrders
/* Author: <ByungsuJung>
** Desc: Extracts and transforms data for FactOrders
** Change Log: When,Who,What
** 20189-01-17,<ByungsuJung>,Created Sproc.
*/
As
  SELECT
   [OrderID] = o.OrderID,
   [CustomerKey] = dc.CustomerKey, 
   [OrderDateKey] = dd.DateKey, 
   [ProductKey] = p.ProductKey, 
   [ActualOrderUnitPrice] = od.UnitPrice, 
   [ActualOrderQuantity] = od.Quantity
  From NorthwindLite.dbo.OrderDetails as od
  Join NorthwindLite.dbo.Orders as o
  On od.OrderID = o.OrderID
  Join DWNorthwindLite.dbo.DimCustomers as dc
  On o.CustomerID = dc.CustomerID
  Join DWNorthwindLite.dbo.DimDates as dd
  On Cast(Convert(nVarchar(50), o.OrderDate, 112) as int) = dd.DateKey
  Join DWNorthwindLite.dbo.DimProducts as p
  On od.ProductID = p.ProductID
go
/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLSyncFactOrders
/* Author: <ByungsuJung>
** Desc: Inserts data into FactOrders
** Change Log: When,Who,What
** 20189-01-17,<ByungsuJung>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
	Merge Into FactOrders as TargetTable
	Using vETLFactOrders as SourceTable
		On TargetTable.OrderID = SourceTable.OrderID And
		   TargetTable.[CustomerKey] = SourceTable.[CustomerKey] And
		   TargetTable.[OrderDateKey] = SourceTable.[OrderDateKey] And
		   TargetTable.[ProductKey] = SourceTable.[ProductKey]
		When Not Matched
			Then
			Insert
			Values (SourceTable.OrderID, SourceTable.[CustomerKey], SourceTable.[OrderDateKey], SourceTable.[ProductKey], SourceTable.[ActualOrderUnitPrice], SourceTable.[ActualOrderQuantity])
		When Matched
		And SourceTable.[CustomerKey] <> TargetTable.[CustomerKey]
			Or SourceTable.[OrderDateKey] <> TargetTable.[OrderDateKey]
			Or SourceTable.[ProductKey] <> TargetTable.[ProductKey]
			Or SourceTable.[ActualOrderUnitPrice] <> TargetTable.[ActualOrderUnitPrice]
			Or SourceTable.[ActualOrderQuantity] <> TargetTable.[ActualOrderQuantity]
			Then
				Update
				Set TargetTable.[CustomerKey] = SourceTable.[CustomerKey],
					TargetTable.[OrderDateKey] = SourceTable.[OrderDateKey],
					TargetTable.[ProductKey] = SourceTable.[ProductKey],
					TargetTable.[ActualOrderUnitPrice] = SourceTable.[ActualOrderUnitPrice],
					TargetTable.[ActualOrderQuantity] = SourceTable.[ActualOrderQuantity]
		When Not Matched By Source
			Then
				Delete
   ;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncFactOrders;
 Print @Status;
*/
go

--********************************************************************--
-- C)  NOT NEEDED FOR INCREMENTAL LOADING: Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--


--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go

Declare @Status int = 0;
Exec @Status = pETLSyncDimProducts;
Select [Object] = 'pETLSyncDimProducts', [Status] = @Status;

Exec @Status = pETLSyncDimCustomers;
Select [Object] = 'pETLSyncDimCustomers', [Status] = @Status;

Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = @Status;

Exec @Status = pETLSyncFactOrders;
Select [Object] = 'pETLFillFactOrders', [Status] = @Status;

go
Select * from [dbo].[DimProducts];
Select * from [dbo].[DimCustomers];
Select * from [dbo].[DimDates];
Select * from [dbo].[FactOrders];



-- Personal Test Codes -- 

--Insert into NorthwindLite.dbo.Products ([ProductName], [CategoryID])
--Values ('xxx',1)
--Select * From northwindlite.dbo.Products
--Select * From DWNorthwindLite_withSCD.dbo.DimProducts
--Go
--Exec pETLSyncDimProducts
--Select * From northwindlite.dbo.Products
--Select * From DWNorthwindLite_withSCD.dbo.DimProducts
--Go

--Update NorthwindLite.dbo.Products
--	Set ProductName = 'yyyyyy'
--	Where ProductName = 'xxx'
--Select * From northwindlite.dbo.Products
--Select * From DWNorthwindLite_withSCD.dbo.DimProducts
--Go
--Exec pETLSyncDimProducts
--Select * From northwindlite.dbo.Products
--Select * From DWNorthwindLite_withSCD.dbo.DimProducts
--Go

--Delete From NorthwindLite.dbo.Products Where ProductName = 'yyyyyy'
--Select * From northwindlite.dbo.Products
--Select * From DWNorthwindLite_withSCD.dbo.DimProducts
--Go
--Exec pETLSyncDimProducts
--Select * From northwindlite.dbo.Products
--Select * From DWNorthwindLite_withSCD.dbo.DimProducts
--Go


--Insert into NorthwindLite.dbo.Customers ([CustomerID],[CompanyName], [ContactName], [Address], [City], [Country] )
--Values ('zzzzz','xxx','vvv','z','z','z')
--Select * From northwindlite.dbo.Customers
--Select * From DWNorthwindLite_withSCD.dbo.DimCustomers
--Go
--Exec pETLSyncDimCustomers
--Select * From northwindlite.dbo.Customers
--Select * From DWNorthwindLite_withSCD.dbo.DimCustomers
--Go
--Update NorthwindLite.dbo.Customers
--	Set CompanyName = 'yyyyy'
--	Where CustomerID = 'zzzzz'
--Select * From northwindlite.dbo.Customers
--Select * From DWNorthwindLite_withSCD.dbo.DimCustomers
--Go
--Exec pETLSyncDimCustomers
--Select * From northwindlite.dbo.Customers
--Select * From DWNorthwindLite_withSCD.dbo.DimCustomers
--Go

--Delete From NorthwindLite.dbo.Customers Where CompanyName = 'yyyyy'
--Select * From northwindlite.dbo.Customers
--Select * From DWNorthwindLite_withSCD.dbo.DimCustomers
--Go
--Exec pETLSyncDimCustomers
--Select * From northwindlite.dbo.Customers
--Select * From DWNorthwindLite_withSCD.dbo.DimCustomers
--Go

--Insert into NorthwindLite.dbo.OrderDetails
--([OrderID], [ProductID], [UnitPrice], [Quantity])
--Values
--(10248,1,11,1)
--Select * From northwindlite.dbo.OrderDetails
--Select * From DWNorthwindLite_withSCD.dbo.FactOrders
--Go
--Exec pETLSyncFactOrders
--Select * From northwindlite.dbo.OrderDetails
--Select * From DWNorthwindLite_withSCD.dbo.FactOrders

--Update NorthwindLite.dbo.OrderDetails 
--Set Quantity = 10
--where (OrderID = 10248 And ProductID = 1)
--Select * From northwindlite.dbo.OrderDetails
--Select * From DWNorthwindLite_withSCD.dbo.FactOrders
--Go
--Exec pETLSyncFactOrders
--Select * From northwindlite.dbo.OrderDetails
--Select * From DWNorthwindLite_withSCD.dbo.FactOrders

--Delete From NorthwindLite.dbo.OrderDetails where(OrderID = 10248 And ProductID = 1)
--Select * From northwindlite.dbo.OrderDetails
--Select * From DWNorthwindLite_withSCD.dbo.FactOrders
--Go
--Exec pETLSyncFactOrders
--Select * From northwindlite.dbo.OrderDetails
--Select * From DWNorthwindLite_withSCD.dbo.FactOrders