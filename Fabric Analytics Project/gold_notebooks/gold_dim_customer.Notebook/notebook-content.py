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

# CELL ********************

def view_table(schema, table):
    display(spark.sql(
        f""" 
            SELECT *
            FROM {schema}.{table}
        """
    ))


view_table('silver', 'crm_cust_info')
view_table('silver', 'erp_cust_az12')
view_table('silver', 'erp_loc_a101')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql(
        """
            SELECT 
                cst.*,
                ag.*,
                loc.*
        
            FROM lh_gold.silver.crm_cust_info cst
            INNER JOIN lh_gold.silver.erp_cust_az12 ag
            ON cst.customer_key = ag.customer_id  
            INNER JOIN lh_gold.silver.erp_loc_a101 loc
            ON cst.customer_key = loc.customer_id 
        """
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check on matching gender columns from crm_cust_info and erp_cust_az12
# crm_cust_info gender is master. If gender is n/a, check erp_cust_az12 gender and replace. If both tables have n/a, return n/a

# CELL ********************

display(
    spark.sql(
        """
            SELECT cst.gender, ag.gender,
            CASE 
                WHEN cst.gender = 'n/a' THEN ag.gender
                ELSE cst.gender
            END AS new_gender
            FROM lh_gold.silver.crm_cust_info cst
            INNER JOIN lh_gold.silver.erp_cust_az12 ag
            ON cst.customer_key = ag.customer_id  
            INNER JOIN lh_gold.silver.erp_loc_a101 loc
            ON cst.customer_key = loc.customer_id 
            WHERE cst.gender <> ag.gender
        """
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Select  Columns for the dim_customer

# CELL ********************


df = spark.sql(
        """
            SELECT 
                ROW_NUMBER()OVER(ORDER BY created_date ASC, cst.customer_id ASC) AS customer_id,
                cst.customer_id AS customer_number,
                cst.customer_key,
                first_name,
                last_name,
                marital_status,
             
                CASE 
                    WHEN cst.gender = 'n/a' THEN ag.gender
                    ELSE cst.gender
                END AS gender,

                birth_date,
                country,
                created_date
        
            FROM lh_gold.silver.crm_cust_info cst
            INNER JOIN lh_gold.silver.erp_cust_az12 ag
            ON cst.customer_key = ag.customer_id  
            INNER JOIN lh_gold.silver.erp_loc_a101 loc
            ON cst.customer_key = loc.customer_id
        """
    )

display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write to Gold layer

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('lh_gold.gold.dim_customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read dim_customer from the gold layer

# CELL ********************

gold_dim_customer_df = spark.table('gold.dim_customer')
display(gold_dim_customer_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
