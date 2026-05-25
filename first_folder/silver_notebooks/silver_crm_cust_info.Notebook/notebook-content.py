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

# init

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, upper, trim, current_date
from pyspark.sql.types import IntegerType, StringType, DataType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read crm_cust_info from bronze


# CELL ********************

df = spark.table("bronze.crm_cust_info") 

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Explore crm_cust_info data

# CELL ********************

df.printSchema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.describe().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check for Null values and white spaces

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

# ## Check cst_create_date column

# CELL ********************

display(df.filter(col('cst_create_date') > current_date()) )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Notes
# 
# - Df has null and whitespaces
# - Null should be replaced with n/a where necessary
# - cst_marital_status and cst_gndr has unstandadized values and null. Change to more friendly values


# MARKDOWN ********************

# # Data Transformation

# CELL ********************

# Removing white spaces using trim 
new_df = df # Defined new_df to track changes in this forloop tranformation 

for column in new_df.schema:
    column_name = column.name
    column_type = column.dataType

    if isinstance(column_type, StringType):
        new_df = new_df.withColumn(
                column_name,
                trim(col(column_name))
            ).fillna('n/a')



check_null_whitespace(new_df)

df = new_df # Assigned new_df back to df after transformation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Map the cst_marital_status and cst_gndr to more friendly values
df = (
    df.withColumn(
        'cst_marital_status',
        when(upper(col('cst_marital_status')) == 'M', 'Married')
        .when(upper(col('cst_marital_status')) == 'S', 'Single')
        .otherwise('n/a')
    ).withColumn(
        'cst_gndr',
        when(upper(col('cst_gndr')) == 'M', 'Male')
        .when(upper(col('cst_gndr')) == 'F', 'Female')
        .otherwise('n/a')
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Rename columns to more friendly values

# CELL ********************

COLUMN_RENAME = {
    'cst_id': 'customer_id', 
    'cst_key': 'customer_key', 
    'cst_firstname': 'first_name', 
    'cst_lastname': 'last_name', 
    'cst_marital_status': 'marital_status', 
    'cst_gndr': 'gender', 
    'cst_create_date': 'created_date'
}

df = df.withColumnsRenamed(COLUMN_RENAME)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transformation done but some Null values still exist in cst_id and cst_create_date. To be explored later

# MARKDOWN ********************

# # Write df to silver layer

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('silver.crm_cust_info')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
