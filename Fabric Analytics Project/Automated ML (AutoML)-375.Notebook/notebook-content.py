# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # A step-by-step guide to understand Automated ML

# MARKDOWN ********************

# ## Introduction
# 
# This tutorial demonstrates how to leverage Automated ML (AutoML) to automate various stages of the machine learning pipeline within the data science workflow in Microsoft Fabric.
# 
# The main steps in this notebook are:
# 
# 1. Load the data
# 2. Understand and preprocess the data
# 3. Train a baseline machine learning model using SynapseML LightGBM
# 4. Create an AutoML trial with FLAML find the best model for the Apache Spark dataset
# 5. Parallelize the AutoML trial with Apache Spark
# 6. Save the final machine learning model
# 
# > [!IMPORTANT]
# > **Automated ML is currently supported on Fabric Runtimes 1.2+ or any Fabric environment with Spark 3.4.**

# MARKDOWN ********************

# ## Step 1: Load the data

# MARKDOWN ********************

# ### Dataset
# 
# The dataset contains churn status of 10,000 customers along with 14 attributes that include credit score, geographical location (Germany, France, Spain), gender (male, female), age, tenure (years of being bank's customer), account balance, estimated salary, number of products that a customer has purchased through the bank, credit card status (whether a customer has a credit card or not), and active member status (whether an active bank's customer or not).
# 
# The dataset also includes columns such as row number, customer ID, and customer surname that should have no impact on customer's decision to leave the bank. The event that defines the customer's churn is the closing of the customer's bank account, therefore, the column `exit` in the dataset refers to customer's abandonment. Since you don't have much context about these attributes, you'll proceed without having background information about the dataset. Your aim is to understand how these attributes contribute to the `exit` status.
# 
# Out of the 10,000 customers, only 2037 customers (around 20%) have left the bank. Therefore, given the class imbalance ratio, it is recommended to generate synthetic data.
# 
# - churn.csv
# 
# |"CustomerID"|"Surname"|"CreditScore"|"Geography"|"Gender"|"Age"|"Tenure"|"Balance"|"NumOfProducts"|"HasCrCard"|"IsActiveMember"|"EstimatedSalary"|"Exited"|
# |---|---|---|---|---|---|---|---|---|---|---|---|---|
# |15634602|Hargrave|619|France|Female|42|2|0.00|1|1|1|101348.88|1|
# |15647311|Hill|608|Spain|Female|41|1|83807.86|1|0|1|112542.58|0|
# 
# 


# CELL ********************

import os
import requests

IS_CUSTOM_DATA = False  # if TRUE, dataset has to be uploaded manually

if not IS_CUSTOM_DATA:
    # Specify the remote URL where the data is hosted
    remote_url = "https://synapseaisolutionsa.z13.web.core.windows.net/data/bankcustomerchurn"

    # List of data files to download
    file_list = ["churn.csv"]

    # Define the download path within the lakehouse
    download_path = "/lakehouse/default/Files/churn/raw"

    # Check if the lakehouse directory exists; if not, raise an error
    if not os.path.exists("/lakehouse/default"):
        raise FileNotFoundError("Default lakehouse not found. Please add a lakehouse and restart the session.")

    # Create the download directory if it doesn't exist
    os.makedirs(download_path, exist_ok=True)

    # Download each data file if it doesn't already exist in the lakehouse
    for fname in file_list:
        if not os.path.exists(f"{download_path}/{fname}"):
            r = requests.get(f"{remote_url}/{fname}", timeout=30)
            with open(f"{download_path}/{fname}", "wb") as f:
                f.write(r.content)

    print("Downloaded demo data files into lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Read raw data from the lakehouse
# 
# Reads raw data from the **Files** section of the lakehouse, adds additional columns for different date parts and the same information will be used to create partitioned delta table.

# CELL ********************

df = spark.read.option("header", True).option("inferSchema", True).csv("Files/churn/raw/churn.csv").cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Data Exploration and Preprocessing

# MARKDOWN ********************

# #### Display raw data
# 
# Explore the raw data with `display`, do some basic statistics and show chart views.

# CELL ********************

display(df, summary=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Data Preprocessing, Cleaning, and Feature Engineering
# 
# In this step, you clean and preprocess the data, addressing missing values and inconsistencies to ensure its quality. Additionally, you'll employ feature engineering techniques to enhance the predictive power of your models.


# CELL ********************

def clean_data(df):
    # Drop rows with missing data across all columns
    df = df.dropna(how="all")
    # Drop duplicate rows in columns: 'RowNumber', 'CustomerId'
    df = df.dropDuplicates(subset=["RowNumber", "CustomerId"])
    # Drop columns: 'RowNumber', 'CustomerId', 'Surname'
    df = df.drop("RowNumber", "CustomerId", "Surname")
    return df


# Create a copy of the original dataframe by selecting all the columns
df_copy = df.select("*")

# Apply the clean_data function to the copy
df_clean = clean_data(df_copy)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# You'll use one-hot encoding on the `Geography` and `Gender` columns to convert categorical variables into numerical representations.

# CELL ********************

# Import PySpark functions
from pyspark.sql import functions as F

# Create dummy columns for Geography and Gender using one-hot encoding
df_clean = df_clean.select(
    "*",
    F.when(F.col("Geography") == "France", 1).otherwise(0).alias("Geography_France"),
    F.when(F.col("Geography") == "Germany", 1).otherwise(0).alias("Geography_Germany"),
    F.when(F.col("Geography") == "Spain", 1).otherwise(0).alias("Geography_Spain"),
    F.when(F.col("Gender") == "Female", 1).otherwise(0).alias("Gender_Female"),
    F.when(F.col("Gender") == "Male", 1).otherwise(0).alias("Gender_Male"),
)

# Drop the original Geography and Gender columns
df_clean = df_clean.drop("Geography", "Gender")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Display cleaned data
# 
# Showcase the cleaned and feature-engineered dataset using the display function.

# CELL ********************

display(df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Save to lakehouse
# 
# Take the cleaned and transformed PySpark DataFrame, `df_clean`, and save it as a Delta table named `churn_data_clean` in the lakehouse. Utilizing the Delta format ensures efficient versioning and management of the dataset. Note that using the `mode("overwrite")`, ensures that any existing table with the same name is overwritten, and a new version of the table is created.

# CELL ********************

# Create PySpark DataFrame from Pandas
df_clean.write.mode("overwrite").format("delta").save(f"Tables/churn_data_clean")
print(f"Spark dataframe saved to delta table: churn_data_clean")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Train a baseline machine learning model

# MARKDOWN ********************

# With your data in place, you can now define the model. You'll train a SynapseML LightGBM model in this notebook. You will also use MLfLow and Fabric Autologging to track the experiments.
# 
# You first need to load the cleaned and feature-engineered dataset from the lakehouse using Delta format.

# CELL ********************

df_final = spark.read.format("delta").load("Tables/churn_data_clean")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Generate train-test datasets and vector assembler
# 
# Split the data into training and test datasets with an 80/20 ratio and prepare the data to train your machine learning model. This preparation involves importing the `VectorAssembler` from PySpark ML to combine feature columns into a single `features` column. Then, you'll use the `VectorAssembler` to transform the training and test datasets, resulting in `train_data` and `test_data` DataFrames containing the target variable `Exited` and the feature vectors. These datasets are now ready for building and evaluating machine learning models.

# CELL ********************

# Import the necessary library for feature vectorization
from pyspark.ml.feature import VectorAssembler

# Train-Test Separation
train_raw, test_raw = df_final.randomSplit([0.8, 0.2], seed=41)

# Define the feature columns (excluding the target variable 'Exited')
feature_cols = [col for col in df_final.columns if col != "Exited"]

# Create a VectorAssembler to combine feature columns into a single 'features' column
featurizer = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Transform the training and testing datasets using the VectorAssembler
train_data = featurizer.transform(train_raw)["Exited", "features"]
test_data = featurizer.transform(test_raw)["Exited", "features"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Set the logging level
# 
# You can configure the logging level to suppress unnecessary outputs from the SynapseML library to keep the logs cleaner.

# CELL ********************

import logging

logging.getLogger("synapse.ml").setLevel(logging.CRITICAL)
logging.getLogger("mlflow.utils").setLevel(logging.CRITICAL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Set up MLflow experiment tracking
# 
# MLflow is an open source platform that is deeply integrated into the Data Science experience in Fabric and allows to easily track and compare the performance of different models and experiments without the need for manual tracking. For more information, see [Autologging in Microsoft Fabric](https://aka.ms/fabric-autologging).

# CELL ********************

import mlflow

# Disable exclusive mode for autologging to track additional metrics
mlflow.autolog(exclusive=False)

# Set the MLflow experiment to "automl_sample" and enable automatic logging
mlflow.set_experiment("sample-automl-experiment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Train and evaluate the baseline model
# 
# Train a `LightGBMClassifier` model on the training data that is configured with appropriate settings for binary classification and imbalance handling. Then make predictions on the test data using this trained model. Predicted probabilities for the positive class and true labels from the test data are extracted, followed by calculation of the ROC-AUC score using sklearn's `roc_auc_score` function.


# CELL ********************

from synapse.ml.lightgbm import LightGBMClassifier
from sklearn.metrics import roc_auc_score

# Assuming you have already defined 'train_data' and 'test_data'

with mlflow.start_run(run_name="default") as run:
    # Create a LightGBMClassifier model with specified settings
    model = LightGBMClassifier(objective="binary", featuresCol="features", labelCol="Exited", dataTransferMode="bulk")

    # Fit the model to the training data
    model = model.fit(train_data)

    # Get the predictions
    predictions = model.transform(test_data)

    # Extract the predicted probabilities for the positive class
    y_pred = predictions.select("probability").rdd.map(lambda x: x[0][1]).collect()

    # Extract the true labels from the 'test_data' DataFrame
    y_true = test_data.select("Exited").rdd.map(lambda x: x[0]).collect()

    # Compute the ROC AUC score
    roc_auc = roc_auc_score(y_true, y_pred)

    # Log the ROC AUC score with MLflow
    mlflow.log_metric("roc_auc", roc_auc)

    # Print or log the ROC AUC score
    print("ROC AUC Score:", roc_auc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Create an AutoML trial with FLAML

# MARKDOWN ********************

# In this section, you'll create an AutoML trial using the FLAML package, configure the trial settings, convert the Spark dataset to a Pandas on Spark dataset, run the AutoML trial, and view the resulting metrics.

# MARKDOWN ********************

# #### Configure the AutoML trial and settings
# 
# Import the required classes and modules from the FLAML package and instantiate AutoML, which automates the machine learning pipeline.

# CELL ********************

# Import the AutoML class from the FLAML package
from flaml import AutoML
from flaml.automl.spark.utils import to_pandas_on_spark

# Create an AutoML instance
automl_spark = AutoML()

# Define AutoML settings
settings = {
    "time_budget": 120,  # Total running time in seconds
    "max_iter": 3,  # Maximum number of trials
    "metric": "roc_auc",  # Optimization metric (ROC AUC in this case)
    "task": "classification",  # Task type (classification)
    "log_file_name": "flaml_experiment.log",  # FLAML log file
    "seed": 41,  # Random seed
    "mlflow_exp_name": "sample-automl-experiment",  # MLflow experiment name
    "verbose": 1,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Convert to Pandas on Spark
# 
# To execute AutoML with a Spark-based dataset, you must convert it to a Pandas on Spark dataset using the `to_pandas_on_spark` function. This ensures FLAML can efficiently work with the data.

# CELL ********************

df_automl = to_pandas_on_spark(train_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Run the AutoML trial
# 
# Execute the AutoML trial, using a nested MLflow run to track the experiment within the existing MLflow run context. The trial is conducted on the Pandas on Spark dataset `df_automl` with the target variable `Exited`, and the defined settings are passed to the `fit` function for configuration.

# CELL ********************

"""The main flaml automl API"""

with mlflow.start_run(nested=True, run_name="spark_automl"):
    automl_spark.fit(dataframe=df_automl, label="Exited", isUnbalance=True, dataTransferMode="bulk", **settings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### View resulting metrics
# 
# Retrieve and display the results of the AutoML trial. These metrics offer insights into the performance and configuration of the AutoML model on the provided dataset.

# CELL ********************

# Retrieve and display the best hyperparameter configuration and metrics
if automl_spark.best_config is None:
    print("No best config found. Try running the AutoML process again with more time budget.")
else:
    print("Best hyperparameter config:", automl_spark.best_config)
    print("Best ROC AUC on validation data: {0:.4g}".format(1 - automl_spark.best_loss))
    print("Training duration of the best run: {0:.4g} s".format(automl_spark.best_config_train_time))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Parallelize your AutoML trial with Apache Spark

# MARKDOWN ********************

# In scenarios where your dataset can fit into a single node and you aim to harness Spark's capabilities for running multiple parallel AutoML trials simultaneously, you can follow these steps:


# MARKDOWN ********************

# #### Convert to Pandas DataFrame
# 
# To enable parallelization, your data must first be converted into a Pandas DataFrame.

# CELL ********************

pandas_df = train_raw.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Configure parallelization settings
# 
# Configure `use_spark` to `True` to enable Spark-based parallelism. By default, FLAML will initiate one trial per executor. You can customize the number of concurrent trials using the `n_concurrent_trials` argument. To learn more about how to parallelize your AutoML trails, you can visit [FLAML documentation for parallel Spark jobs](https://microsoft.github.io/FLAML/docs/Examples/Integrate%20-%20Spark#parallel-spark-jobs).

# CELL ********************

# Create an AutoML instance
automl = AutoML()

# Set MLflow experiment
mlflow.set_experiment("sample-automl-experiment-spark")

# Define settings
settings = {
    "time_budget": 120,  # Total running time in seconds
    "max_iter": 3,  # Maximum number of trials
    "metric": "roc_auc",  # Optimization metric (ROC AUC in this case)
    "task": "classification",  # Task type (classification)
    "seed": 41,  # Random seed
    "use_spark": True,  # Enable Spark-based parallelism
    "n_concurrent_trials": 3,  # Number of concurrent trials to run
    "force_cancel": True,  # Force stop training once time_budget is used up
    "mlflow_exp_name": "sample-automl-experiment-spark",  # MLflow experiment name
    "verbose": 1,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Run the AutoML trial
# 
# Execute the AutoML trial in parallel with the specified settings. Note that a nested MLflow run will be utilized to track the experiment within the existing MLflow run context.

# CELL ********************

"""The main flaml automl API"""
with mlflow.start_run(nested=True, run_name="parallel_trial"):
    automl.fit(dataframe=pandas_df, label="Exited", **settings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Understand AutoML runs
# 
# The `flaml.visualization` module provides functions for plotting and comparing runs in FLAML. Users can utilize Plotly to interact with their AutoML experiment plots. A **feature importance plot** is a valuable visualization tool enabling you to grasp the significance of various input features in determining the predictions of the final, best model.

# CELL ********************

import flaml.visualization as fviz

fig = fviz.plot_feature_importance(automl)

if fig is None:
    print("No feature importance plot available. Try running the AutoML process again with more time budget.")
else:
    fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### View metrics
# 
# Upon completion of the parallel AutoML trial, retrieve and showcase the results, including the best hyperparameter configuration, ROC-AUC on the validation dataset, and the training duration of the top-performing run.

# CELL ********************

""" retrieve best config"""
if automl.best_config is None:
    print("No best config found. Try running the AutoML process again with more time budget.")
else:
    print("Best hyperparmeter config:", automl.best_config)
    print("Best roc_auc on validation data: {0:.4g}".format(1 - automl.best_loss))
    print("Training duration of best run: {0:.4g} s".format(automl.best_config_train_time))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Experiments artifact for tracking model performance
# 
# The experiment runs are automatically saved in the experiment artifact that can be found from the workspace. They're named based on the name used for setting the experiment. All of the trained models, their runs, performance metrics and model parameters are logged as can be seen from the experiment page shown in the image below.   
# 
# To view your experiments:
# 1. On the left panel, select your workspace.
# 1. Find and select the experiment name, in this case _sample-automl-experiment_.
# 
# <img src="https://synapseaisolutionsa.z13.web.core.windows.net/data/AutoML_nested_details.png"  width="400%" height="100%" title="Screenshot shows logged values for one of the models.">


# MARKDOWN ********************

# ## Step 6: Save as the final machine learning model

# MARKDOWN ********************

# Upon completing the AutoML trial, you can now save the final, tuned model as an ML model in Fabric.

# CELL ********************

# Specify the model name and the path where you want to save it in the registry
model_name = "churn_model"  # Replace with your desired model name

if automl.best_run_id is None:
    print("No best run ID found. Try running the AutoML process again with more time budget.")
    registered_model = None
else:
    model_path = f"runs:/{automl.best_run_id}/model"
    # Register the model to the MLflow registry
    registered_model = mlflow.register_model(model_uri=model_path, name=model_name)
    # Print the registered model's name and version
    print(f"Model '{registered_model.name}' version {registered_model.version} registered successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7: Predict with the saved model

# MARKDOWN ********************

# Microsoft Fabric allows users to operationalize machine learning models with a scalable function called `PREDICT`, which supports batch scoring (or batch inferencing) in any compute engine.
# 
# You can generate batch predictions directly from the Microsoft Fabric notebook or from a given model's item page. For more information on how to use `PREDICT`, see [Model scoring with PREDICT in Microsoft Fabric](https://aka.ms/fabric-predict).
# 
# 1. Load the better-performing model (*Version 2*) for batch scoring and generate the prediction results.

# CELL ********************

display(test_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from synapse.ml.predict import MLFlowTransformer

if registered_model is None:
    print("No registered model found. Please ensure the AutoML process completed successfully.")
    batch_predictions = None
else:
    model = MLFlowTransformer(
        inputCols=feature_cols,
        outputCol="prediction",
        modelName=model_name,
        modelVersion=registered_model.version,
    )

    batch_predictions = model.transform(test_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(batch_predictions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 2. Save predictions into the lakehouse.

# CELL ********************

# Save the predictions into the lakehouse
if batch_predictions is None:
    print("No predictions to save. Ensure the model was registered and predictions were generated successfully.")
else:
    batch_predictions.write.format("delta").mode("overwrite").save("Files/churn/predictions/batch_predictions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
