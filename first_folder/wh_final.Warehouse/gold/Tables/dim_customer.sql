CREATE TABLE [gold].[dim_customer] (

	[customer_id] int NULL, 
	[customer_number] int NULL, 
	[customer_key] varchar(8000) NULL, 
	[first_name] varchar(8000) NULL, 
	[last_name] varchar(8000) NULL, 
	[marital_status] varchar(8000) NULL, 
	[gender] varchar(8000) NULL, 
	[birth_date] date NULL, 
	[country] varchar(8000) NULL, 
	[created_date] date NULL
);