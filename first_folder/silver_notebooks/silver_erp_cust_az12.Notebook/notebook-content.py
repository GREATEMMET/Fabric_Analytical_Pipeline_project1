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
from pyspark.sql.functions import when, col, trim, upper
from pyspark.sql.types import IntegerType, StringType, DataType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Data from bronze layer

# CELL ********************

df = spark.table('bronze.erp_cust_az12')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Explore Data

# MARKDOWN ********************

# ## Check for whitespace and null values

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

# ## Check for distinct values in the GEN column

# CELL ********************

display(
    df.select(col('GEN')).distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check for unmatching and matching ids

# CELL ********************

query = """

    SELECT (SELECT COUNT(*) 
    FROM lh_silver.bronze.erp_cust_az12
    WHERE CID NOT IN (SELECT cst_key FROM lh_silver.bronze.crm_cust_info)) unmatching_key_count,
    (SELECT COUNT(*)
    FROM lh_silver.bronze.crm_cust_info
    WHERE cst_key IN (SELECT CID FROM lh_silver.bronze.erp_cust_az12)) matching_key_count
"""


display(spark.sql(query))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View customer_id from crm_cust_info and erp_cust_az12 side by side 
display(spark.sql(
    """
        SELECT CID, customer_key
        FROM lh_silver.bronze.erp_cust_az12 AS dg
        JOIN lh_silver.silver.crm_cust_info AS cust
    """
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check dates and make sure no customer above the age 120

# CELL ********************

display(
    df.withColumn('age_years', F.floor(F.months_between(F.current_date(), col('BDATE'))/12)).filter(col('age_years') > 120)
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Notes
# 
# - whitespaces and null values found in GEN
# - unfriendly column names 
# - GEN column has multiple variations of Male and Female
# - CID starting with 'NAS' do not match customer_ids from crm_cust_info table, resulting in 11046 unmatched ids


# MARKDOWN ********************

# # Transformations

# MARKDOWN ********************

# ## Remove whitespace and vull values 

# CELL ********************

for column in df.schema:
    column_name = column.name
    column_type = column.dataType
    if isinstance(column_type, StringType):
        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Remap the GEN column F:Female, M:Male, "":n/a, Null:n/a

# CELL ********************

df = df.withColumn(
    'GEN',
    F.when(upper(col('GEN')).isin("M", 'MALE'), "Male")
    .when(upper(col('GEN')).isin("F", "FEMALE"), "Female")
    .otherwise('n/a')
)

display(df.select(col('GEN')).distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create new customer_id column, removing 'NAS' 

# CELL ********************


df = df.withColumn(
    'customer_id',
    F.when(col('CID').like('NAS%'), F.expr("substring(CID, 4, length(CID))") )
    .otherwise(col('CID'))
       
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView('cust_az12')

## Writing a CTE function to validated the transformation. Checking if we have more matches after removing NAS

display(spark.sql("""
    WITH full_j AS (
    SELECT * 
    FROM  lh_silver.bronze.crm_cust_info
    FULL OUTER JOIN cust_az12
    ON customer_id = cst_key
),
matched AS (
    SELECT *
    FROM full_j
    WHERE customer_id IS NOT NULL
      AND cst_key IS NOT NULL
),
unmatched AS (
    SELECT *
    FROM full_j
    WHERE customer_id IS NULL
       OR cst_key IS NULL
)
SELECT 
    (SELECT COUNT(*) FROM matched) AS matched_keys,
    (SELECT COUNT(*) FROM unmatched) AS unmatched_key

"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Rename the columns to more friendly names

# CELL ********************

COLUMNS_REMAP = {
    'CID':'old_customer_id',
    'BDATE':'birth_date',
    'GEN':'gender'
}

df = df.withColumnsRenamed(COLUMNS_REMAP)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write to silver layer

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('silver.erp_cust_az12')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read from silver layer

# CELL ********************

silver_erp_cust_az12_df = spark.table('silver.erp_cust_az12')
display(silver_erp_cust_az12_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
