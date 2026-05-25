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
from pyspark.sql.functions import col, when, trim, upper
from pyspark.sql.types import StringType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run Data_Quality_Utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read data from bronze 

# CELL ********************

df = spark.table('bronze.erp_loc_a101')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Explore data 

# CELL ********************

check_whitespace_and_null(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cheak for distinct Countrys

# CELL ********************

display(
    df.select(col('CNTRY')).distinct()
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Notes
# - Null values and whitespaces found in CNTRY
# - CID has "-" which do not match Primary keys from other customer table
# - Some country have different variants 

# MARKDOWN ********************

# # Transformations

# MARKDOWN ********************

# ## Remove the "-" in the CID 

# CELL ********************

df = df.withColumn(
    'CID',
    regexp_replace(col('CID'), "-", "")
)

display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Standadizing the CNTRY column

# CELL ********************

df = (df.withColumn(
    'country',
    when(trim(upper(col('CNTRY'))).isin('GERMANY','DE'), 'Germany')
    .when(trim(upper(col('CNTRY'))).isin('US','USA', 'UNITED STATES'), 'United States of America')
    .when(trim(upper(col('CNTRY'))).isin(""), 'n/a')
    .when(col('CNTRY').isNull(), 'n/a')
    .otherwise(col('CNTRY'))
))

display(
    df.select('CNTRY', 'country').distinct()
)

display(df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop the CNTRY column

df = df.drop('CNTRY')

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Rename columns into more friendly values

# CELL ********************

df = df.withColumnRenamed( 'CID', 'customer_id')
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write data to Silver layer

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('silver.erp_loc_a101')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read from Silver layer

# CELL ********************

silver_erp_loc_a101 = spark.table('silver.erp_loc_a101')
display(silver_erp_loc_a101)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
