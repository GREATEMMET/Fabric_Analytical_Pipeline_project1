CREATE TABLE [gold].[dim_product] (

	[product_id] int NULL, 
	[product_number] int NULL, 
	[product_key] varchar(8000) NULL, 
	[product_name] varchar(8000) NULL, 
	[product_line] varchar(8000) NULL, 
	[category_id] varchar(8000) NULL, 
	[category] varchar(8000) NULL, 
	[sub_category] varchar(8000) NULL, 
	[cost] int NULL, 
	[maintenance] varchar(8000) NULL, 
	[start_date] date NULL
);