# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ed515e48-a385-4534-b24d-e69349a84549",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "0d2db1d8-2bee-419b-857d-cb6c72a5249c",
# META       "known_lakehouses": [
# META         {
# META           "id": "ed515e48-a385-4534-b24d-e69349a84549"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run Import_Utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read all Product data and join tables

# CELL ********************

## See all columns of the Joined tables

display(spark.sql(
    """
        SELECT 
            prd.*,
            cat.*
        FROM lh_gold.silver.crm_prd_info prd
        INNER JOIN lh_gold.silver.erp_px_cat_g1v2 cat
        USING (category_id)
    """
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(
    """
        SELECT 
            row_number() OVER(ORDER BY start_date ASC, product_id ASC) AS product_id,
            product_id AS product_number,
            -- category_product_key, ## Remove the category_product_key because we already have them in the table
            
            prd.product_key,
            product_name,
            product_line,

            cat.category_id AS category_id,
            category,
            sub_category,

            cost,
            maintenance,
            start_date
        FROM lh_gold.silver.crm_prd_info prd
        INNER JOIN lh_gold.silver.erp_px_cat_g1v2 cat
        USING (category_id)
    """
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write dim_product to the gold layer

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('lh_gold.gold.dim_product')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Gold dim_product

# CELL ********************

gold_dim_product_df = spark.table('lh_gold.gold.dim_product')
display(gold_dim_product_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
