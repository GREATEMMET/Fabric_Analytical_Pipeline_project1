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
from pyspark.sql.functions import when, col, upper, trim, lead, lag
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read from crm_prd_info the bronze 

# CELL ********************

df = spark.table("bronze.crm_prd_info")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Explore crm_prd_info  

# MARKDOWN ********************

# ## Check for columns with Nulls and Whitespaces

# CELL ********************

def check_null_whitespace(dataframe):
    for column in dataframe.schema:
        column_name = column.name
        column_type = column.dataType 

        null_df = dataframe.filter(col(column_name).isNull())
        null_count = null_df.count()

        if null_count:
                print(f"Column {column_name} has a null count of {null_count}")
                display(null_df)

        if isinstance(column_type, StringType):
            white_space_df = dataframe.filter(trim(col(column_name)) != col(column_name))
            white_space_count = white_space_df.count()

            if white_space_count:
                print(f"Column {column_name} has a whitespace count of {white_space_count}")
                display(white_space_df)
        

check_null_whitespace(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check for distinct values of product line

# CELL ********************

df.select("prd_line").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check for compliance. prd_cost should be 0 or above but never negative value or Null

# CELL ********************

df.filter((col("prd_cost") < 0) | (col('prd_cost').isNull())).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Notes
# - White space characters and null values found in some columns
# - prd_line has 5 distict values but no friendly names and null present. Replace Null with n/a and rename values 
# - prd_end_date is before prd_start_date. Needs fixing 
# - prd_cost has 2 Null values. Should be replaced with 0 since in a numerical column
# - Create a new column for prd_cost to have only the first 5 characters

# MARKDOWN ********************

# # Transformations

# MARKDOWN ********************

# ## Remove white spaces

# CELL ********************

for column in df.schema:
    if isinstance(column.dataType, StringType):
        df = df.withColumn(column.name, trim(col(column.name)))


check_null_whitespace(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Replace Null with Zero in prd_cost

# CELL ********************

df = df.fillna({"prd_cost": 0})  # All null values in prd_cost replace with 0

df.where(col('prd_cost').isNull()).show() # Validates the above transformation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
    'prd_line',
    when(trim(upper(col('prd_line'))) == "M", 'Mountain')
    .when(trim(upper(col('prd_line'))) == "R", 'Road')
    .when(trim(upper(col('prd_line'))) == "S", 'Sport')
    .when(trim(upper(col('prd_line'))) == "T", 'Touring')
    .otherwise('n/a')
)



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

# ## Fix data columns: Make sure prd_end_dt comes after prd_start_dt
# According to business rule, the prd_start_dt is true and prd_end_dt should be cleaned to come after. Null values in prd_end_dt means product has not ended yet. 

# CELL ********************

df = df.withColumn(
    'prd_end_dt',
    when(
        col('prd_end_dt').isNotNull(),
        # Apply lead window function to follow leading date from the prd_start_dt column. Subtract a day (-1) from it to avoid overlapping
        lead(col('prd_start_dt'), 1).over(Window.partitionBy('prd_key').orderBy(col('prd_start_dt')))  
    )
)


df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# The above table is using the Type 2 SCD so we can track historical data. But in this case, we are looking for most recent data. Therefore, we have to filter out the old price data and keep the products where prd_end_dt is Null, since they are the most recent data. Drop prd_end_dt column since our filter returns only null prd_end_dt.

# CELL ********************

df = df.filter(col('prd_end_dt').isNull()).drop(col('prd_end_dt'))
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extract the category_id into a new column

# CELL ********************

df = df.withColumn(
    'category_id',
    F.substring(col('prd_key'), 0, 5)
)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create new column for product_key (without the category_id) so it can match sales_details in the gold layer

# CELL ********************

df = df.withColumn(
        'new_prd_key',
        F.substring(col('prd_key'), 7, 100)
    )


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

# ## Rename Columns to more friendly names

# CELL ********************

COLUMN_RENAME = {
    'prd_id':'product_id',
    'prd_key':'category_product_key',
    'new_prd_key': 'product_key',
    'prd_nm':'product_name',
    'prd_cost':'cost',
    'prd_line':'product_line',
    'prd_start_dt':'start_date',
}

df = df.withColumnsRenamed(COLUMN_RENAME)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write to the Silver layer

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('silver.crm_prd_info')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
