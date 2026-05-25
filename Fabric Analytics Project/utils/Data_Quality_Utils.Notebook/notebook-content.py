# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

            whitespace_df = dataframe.filter(

                trim(col(column_name)) != col(column_name)

            )

            whitespace_count = whitespace_df.count()

            if whitespace_count:

                print(f"Column {column_name} has a whitespace count of {whitespace_count}")

                display(whitespace_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
