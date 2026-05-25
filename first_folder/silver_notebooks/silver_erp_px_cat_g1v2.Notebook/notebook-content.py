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

# CELL ********************

%run Data_Quality_Utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Files from bronze

# CELL ********************

df = spark.table('bronze.erp_px_cat_g1v2')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Explore the data

# MARKDOWN ********************

# ## Check for Whitespaces or Null

# CELL ********************

check_whitespace_and_null(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check if the ID matches the ID from crm_prd_info

# CELL ********************

df.createOrReplaceTempView('erp_px_cat_g1v2')

display(spark.sql(

    """
        SELECT *
        FROM erp_px_cat_g1v2
        WHERE ID NOT IN (SELECT category_id FROM lh_silver.silver.crm_prd_info)
    """
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check the CAT column for disntict values

# CELL ********************

display(df.select(col('CAT')).distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check the MAINTENANCE column for disntict values

# CELL ********************

display(df.select(col('MAINTENANCE')).distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Notes
# -  There is product mismatch because ID in erp_px_cat_g1v2 does not match product_key in crm_prd_info
# - Need to fix this from the crm_prd_info table.
# - Also replace the "_" with "-" in ID column of the erp_px_cat_g1v2 table

# MARKDOWN ********************

# # Transformation

# CELL ********************

df = df.withColumn(
    'ID',
    regexp_replace(col('ID'), "_", "-")
)

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validation

# CELL ********************

df.createOrReplaceTempView('erp_px_cat_g1v2')

display(spark.sql(

    """
        SELECT *
        FROM erp_px_cat_g1v2
        WHERE ID NOT IN (SELECT category_id FROM lh_silver.silver.crm_prd_info)
    """
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Renaming the Columns

# CELL ********************

COLUMN_RENAME = {
    'ID':'category_id',
    'CAT':'category',
    'SUBCAT':'sub_category',
    'MAINTENANCE':'maintenance'
}

df = df.withColumnsRenamed(COLUMN_RENAME)


display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write the the Silver Layer

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('lh_silver.silver.erp_px_cat_g1v2')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read from Silver Layer

# CELL ********************

silver_erp_px_cat_g1v2_df = spark.table('silver.erp_px_cat_g1v2')

display(silver_erp_px_cat_g1v2_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
