CREATE TABLE [gold].[fact_sales] (

	[order_number] varchar(8000) NULL, 
	[product_id] int NULL, 
	[customer_id] int NULL, 
	[order_date] date NULL, 
	[shipping_date] date NULL, 
	[due_date] date NULL, 
	[sales] int NULL, 
	[quantity] int NULL, 
	[price] int NULL
);