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

# ## View the sales table

# CELL ********************

display(spark.sql(
    """
        SELECT *
        FROM lh_gold.silver.crm_sales_details
    """
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Joining gold tables

# CELL ********************

display(spark.sql(
    """
        SELECT sls.*, cust.*, prd.*
        FROM lh_gold.silver.crm_sales_details AS sls
        INNER JOIN lh_gold.gold.dim_customer AS cust
        ON sls.customer_id = cust.customer_id
        INNER JOIN lh_gold.gold.dim_product AS prd 
        ON prd.product_key = sls.product_key 
    """
))

"""
    There is an issue with joining the product table to the sales table. 
    This is because product keys do not match. 
    Will be fixed in the crm_prd_info of silver layer.
    ----
    The fix was done, hence the reason this runs smoothly now.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Select a sample from the prd_key in the crm_prd_info and compare it against the product key from crm_salesa-details

# display(spark.sql(
#     """
#         SELECT 
#             prd.category_id,
#             prd.product_key AS prd_product_key,
#             sls.product_key AS sls_product_key
#         FROM lh_gold.gold.dim_product AS prd 
#         INNER JOIN lh_gold.silver.crm_sales_details AS sls
#         ON prd.product_key LIKE CONCAT('%' ,sls.product_key ,'%')

#     """

# ))

# """
#     From this, we need to remove the category_id from the product key to make it match.
#     Will be done in the silver layer
# """

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = spark.sql(
    """
        SELECT 
        sls.order_number, 
        prd.product_id,
        cust.customer_id,

        sls.order_date,
        sls.shipping_date,
        sls.due_date,

        sls.sales,
        sls.quantity,
        sls.price

        FROM lh_gold.silver.crm_sales_details AS sls
        INNER JOIN lh_gold.gold.dim_customer AS cust
        ON sls.customer_id = cust.customer_number
        INNER JOIN lh_gold.gold.dim_product AS prd 
        ON prd.product_key = sls.product_key 
    """
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

# # Write to the Gold Layer

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('lh_gold.gold.fact_sales')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read from gold

# CELL ********************

gold_fact_sales_df = spark.table('lh_gold.gold.fact_sales')

display(gold_fact_sales_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
