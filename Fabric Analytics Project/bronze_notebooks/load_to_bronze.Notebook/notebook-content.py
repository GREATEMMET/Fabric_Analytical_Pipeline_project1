# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2570465-df8b-4df5-9064-8f6f34c385d7",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "0d2db1d8-2bee-419b-857d-cb6c72a5249c",
# META       "known_lakehouses": [
# META         {
# META           "id": "d2570465-df8b-4df5-9064-8f6f34c385d7"
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

def define_file_config(src, raw_file_name):
    return (
        {
            'source_path':f'Files/source_{src}/{raw_file_name}.csv',
            'table_path':f'bronze.{raw_file_name.lower()}'
        }
    )


FILE_CONFIG = [
    define_file_config("crm", "crm_cust_info"),
    define_file_config("crm", "crm_prd_info"),
    define_file_config("crm", "crm_sales_details"),
    define_file_config("erp", "erp_CUST_AZ12"),
    define_file_config("erp", "erp_LOC_A101"),
    define_file_config("erp", "erp_PX_CAT_G1V2")
]

for i in FILE_CONFIG:
    print(i)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for item in FILE_CONFIG:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(item["source_path"])

    df.write.mode('overwrite').format('delta').saveAsTable(item['table_path'])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for item in FILE_CONFIG:
    df = spark.table(item["table_path"])
    print(df.count())
    display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
