# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b05f4a0e-99e8-4417-8d74-a58f3e920b95",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "0d2db1d8-2bee-419b-857d-cb6c72a5249c",
# META       "known_lakehouses": [
# META         {
# META           "id": "b05f4a0e-99e8-4417-8d74-a58f3e920b95"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Imports

# CELL ********************

# Init

from pyspark.sql import functions as F
from pyspark.sql.functions import when, trim, col, upper, lit, to_date, round
from pyspark.sql.types import DateType, IntegerType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read from the bronze layer

# CELL ********************

df = spark.table("bronze.crm_sales_details")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Explore the dataset

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.describe())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check for Whitespaces and Null values

# CELL ********************

def check_whitespace_and_null(dataframe):
    for column in dataframe.schema:
        column_name = column.name
        column_type = column.dataType

        null_df = dataframe.filter(col(column_name).isNull())
        null_count = null_df.count()

        if null_count:
                print(f"Column {column_name} has a null count of {null_count}")
                display(null_df)


        if isinstance(column_type, StringType):
            whitespace_df = dataframe.filter(trim(col(column_name)) != col(column_name))
            whitespace_count = whitespace_df.count()

            if whitespace_count:
                print(f"Column {column_name} has a whitespace count of {whitespace_count}")
                display(whitespace_df)


        
check_whitespace_and_null(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# ## Check that date character are 8 and not more or less


# CELL ********************

def check_date_char_length(column):

    improper_date_length = df.filter(
        (F.length(col(column).cast('string')) != 8)
        | (col(column).isNull())
    )

    if improper_date_length.count():
        print(f"Improper date length found in {column}")
        display(improper_date_length)

    else: print (f"No improper date length found in {column}")

    


check_date_char_length('sls_order_dt')
check_date_char_length('sls_ship_dt')
check_date_char_length('sls_due_dt')
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check if sls_ship_dt is before sls_due_dt and sls_ship-dt

# CELL ********************

df.filter(
    ~(col('sls_order_dt') < col('sls_ship_dt'))
    | ~(col('sls_order_dt') < col('sls_due_dt'))
    | ~(col('sls_ship_dt') < col('sls_due_dt'))

).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check that sls_sales is a product of sls_price and sls_quanity

# CELL ********************

display(
    df.filter(
        (col('sls_sales') != col('sls_quantity') * col('sls_price'))
        | (col('sls_sales') < 0)
        | (col('sls_sales').isNull())
        | (col('sls_quantity') < 1)
        | (col('sls_quantity').isNull())
        | (col('sls_price') < 0)
        | (col('sls_price').isNull())
    ).select(
        'sls_price',
        'sls_quantity',
        'sls_sales'
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Notes
# - Dates are of IntergerType.. Needs to be converted to date format.
# - sls_order_dt has zero values and values with character count lessthan 8. Should be converted to Null. sls_ship_dt and sls_due_dt seem to be fine.
# - sls_ship_dt is before sls_due_dt and sls_ship-dt. This is as supposed.
# - We have very inconsistent price values. Some Nulls, Zeros and improper computation. sls_price * sls_quantity must be equal to sls_sales.


# MARKDOWN ********************

# # Transformations 


# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Convert dates from IntergerType to DateType

# CELL ********************

def covert_to_date(df, *columns): # Using *args params
    """
        This function receive takes the dataframe and suppose date columns as arguments and coverts the date columns from IntergerType to DateType.
    """
    for column in columns:
        df = df.withColumn(
            column,
            when(F.length(col(column).cast('string')) != 8, None)
            .otherwise(to_date(col(column).cast('string'), 'yyyyMMdd'))    
    )

    return df

df = covert_to_date(df, 'sls_order_dt', 'sls_due_dt', 'sls_ship_dt')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Standadizing the price, quantity, and sales columns 

# CELL ********************

df = df.withColumn(
    'sls_sales',
    when(
        (col('sls_sales').isNull() )
        | (col('sls_sales') <= 0)
        | (col('sls_sales')!= col('sls_quantity') * F.abs(col('sls_price'))),
        F.abs(col('sls_price')) * col('sls_quantity')
    )
    .otherwise(col('sls_sales'))


).withColumn(
    'sls_price',
     round(col("sls_sales") / col("sls_quantity")).cast("int")
)


display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Because Some order_dates have Null values and that shouldnt be, let filter out orderdates with Null

# CELL ********************

df = df.filter(col('sls_order_dt').isNotNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Rename Columns 


# CELL ********************

COLUMNS_RENAMED = {
    'sls_ord_num': 'order_number',
    'sls_prd_key': 'product_key',
    'sls_cust_id': 'customer_id',
    'sls_order_dt': 'order_date',
    'sls_ship_dt': 'shipping_date',
    'sls_due_dt': 'due_date',
    'sls_sales': 'sales',
    'sls_quantity': 'quantity',
    'sls_price': 'price',
}

df = df.withColumnsRenamed(COLUMNS_RENAMED)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write to the silver layer

# CELL ********************

df.write.format('delta').mode('overwrite').saveAsTable('silver.crm_sales_details')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read from the silver layer

# CELL ********************

silver_crm_sales_details = spark.table('silver.crm_sales_details')

display(silver_crm_sales_details)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
