# Spark on Azure Machine Learning

This repository contains several examples on using Spark clusters for your project work. The contents are the following:

- `create-environment-for-sparknlp/` contains a notebook and a YAML file to create a container image that has `spark-nlp` Python library
- `interactive-spark-with-sparknlp/` contains a notebook to run in a serverless interactive Spark cluster that uses `spark-nlp`. Follow the [instructions below](#use-spark-nlp-on-interactive-and-jobs) to set up the environment before you attempt to run this
- `simple-interactive-spark` contains a notebook that has some sample spark code for the project, with the data locations. This will run as-is in an interactive cluster
- `simple-spark-job-with-parameters` contains a control plane notebook to define a standard Spark job, and a Pyspark script to be executed as the job. This demonstrates how to use parameters in your scripts and jobs. 
- `spark-job-with-sparknlp` containts a control plane notebook to run a job with `spark-nlp`, and a Pyspark script with spark-nlp to be executed as the job. You need to create the environment and download the spark jars before you do this, otherwise the job will fail.

## Group Azure Machine Learning Workspaces

The group AzureML environments are set up. Please read the important information contained in this note carefully.

- The professors have access to your resource groups and workspaces. We will be monitoring for unauthorized services and/or large deployments and we will delete them if we find them.
- Each group has approximately a $300 budget for the project. This allocation is more than enough for your team to complete the project by the end of the semester. No additional credits will be added to any group under any circumstance. Running out of credits is not an excuse to not complete the project on time. You must find a way to complete and fund it yourself if necessary. You must monitor the consumption carefully and plan accordingly. You must make frequent commits and pushes to github.
- This is a shared workspace, meaning that the files in the Notebooks/Files pane (the workspace filesystem) are accessible by everyone in the workspace. This can cause some confusion with the repository and we will provide guidance on how to work in the shared environment.
- If more than one student is working on a Spark notebook attached to a Serverless cluster, that means **you are running (and paying for) more than one cluster at a time**.
- All intermediate large datasets should be written to the Azure Blob Storage account associated with the workspace by using the following command: `df.write.parquet("azureml://datastores/workspaceblobstore/paths/<PATH-FOR_OUTPUT>")`. All members have read/write access to the workspace blobstore.
- You can access your team's workspace by navigating to [https://ml.azure.com](https://ml.azure.com) and going to the Workspaces blade. You should see your individual workspace created earlier in the semester, and your group workspace created by me in the form of group-xx-aml. (Note: we may remove all your prior individual subscriptions so you may not see them anymore.)

## Some considerations when using Spark

- Use interactive Spark sessions **only** for initial development with a small subset of data. You should only use a 1-driver, 2-executor of 4-core machines interactive cluster (12 cores total). Once you are ready to scale out, you should use jobs. DO NOT SCALE OUT INTERACTIVELY. 
- **The cost of the Spark cluster (interactive or job) is $0.143 per core-hour (including driver).** A 1-driver-node/2-worker-node cluster using 4-core machines will cost $0.143 x 12 = $1.716 per hour (prorated to the minute).
- There are other Azure Machine Learning costs in addition to Spark: each compute instance (~0.29/hr for the size mentioned above) plus approximately $0.65-$0.75/day in the platform services associated with AzureML (load balancer, storage, etc.). The platform fees are fixed, you cannot control that.
- You have a limit of 100 running cores maximum for Spark (this does not include the compute instance, those are separate). At this limit, the maximum sized single cluster you can run is 1-driver/24-worker (4-core), but that means that this cluster costs $14.30 per hour! You should not need that large of a cluster. If you do, then something is not right.
- You must cache appropriately to avoid multiple reads.
- Once you get your data to a manageable size, **do not use a spark cluster for doing Python only activities (i.e. visualization, duckdb, etc.)**. You can use your compute instance and we will show how to read files from the workspace blob store into Python.


## Log into your team's Azure Machine Learning (AzureML) Workspace

1. Navigate to https://ml.azure.com and login with your GU credentials
1. Click workspaces
1. You should see your team's workspace named project-group-##


## Your compute instance

Compute instances were created for each team member. These compute instances are individual, and only the named person can use them. You need to use the compute instances for any github operation.

### Do this the first time you use a new compute instance

The first time you start your Compute Insance, you should run the following commands from the terminal of that machine. Change the values in <> to your own. The netid is without `@georgetown.edu`. You only need to run these commands once (but if you add another compute instance, you'll need to do it again.)

```bash
az upgrade
az extension remove -n azure-cli-ml
az extension remove -n ml
az extension add -n ml -y
git config --global --add safe.directory "/home/azureuser/cloudfiles/code/Users/<NETID>/*"
git config --global user.email "<YOUR-EMAIL>"
git config --global user.name "<YOUR-NAME>"
conda activate azureml_py310_sdkv2
pip install azure-ai-ml --upgrade
conda deactivate
```

## Project Data location

The Azure Blob location that has the project data is: `wasbs://reddit-project@dsan6000fall2024.blob.core.windows.net/<DIRECTORY>/`.

- We've added the Reddit data for this year's project which spans the June-2023 to July-2024 in the `202306-202407` directory
- We've also added data from prior years that spans Jan-2021 to March-2023 in the `202101-202303` directory

Within each dataset directory the structure is similar. There is a `comments` and `submissions` subdirectory, and then the data is partitioned by year and month.

- The `202306-202407` dataset uses the `yyyy=####/mm=##` partitioning schema
- The `202101-202303` dataset uses the `year=####/month=##` partitioning schema

**Note:** There are differences in the both datasets with the partitioning schema names, the actual individual parquet file names, and the field names. If you do end up using both sets and want to stack them together (noting there is a three month gap), you'll have to process them individually and generate a common schema.

## Use `spark-nlp` on interactive and jobs

This section will show how to use `spark-nlp` on both interactive and job Spark clusters on Azure Machine Learning. Make sure you've run the commands in the [section above](#do-this-the-first-time-you-use-a-new-compute-instance).

### Edit your project repo `.gitignore`

Add `*.jar` to your repo's `.gitignore` because you will have to download a 600MB file and you can't commit that.

### Create the environment

Run the notebook called `create-environment.ipynb` in the `create-environment-for-sparknlp` directory in Python to create an environment with Python spark-nlp to be used with an interactive cluster and a spark job. You will use this environment for both the interactive cluster and job cluster.

### Configure the interactive Spark session

- Open the notebook you want to run in the interactive Spark cluster in the web portal of Azure Machine Learning
- Attach it to Serverless Spark Compute
- Before you run anything, click on _configure session_
    - In the settings tab, add a configuration value with spark,jars.packages as Property and com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1 as Value
    - In the Python packages tab, select the Custom environment and then scroll down and select the environment you created in step 4 (named sparknlp-python-env:1)
- Click _Apply_
- Run a cell in your notebook to spin up the interactive cluster. Becasue you are using a custom environment, the startup time can be several minutes (8-10 min)

### Configure a Spark job to use additional libraries

**Note: launching a job with external dependencies is now working using the Python SDK.**

- Open the `spark-job-with-sparknlp/azureml-sparknlp-job-control-plane.ipynb` notebook
- Make sure you run the cell that downloada and save the spark-nlp jar
- Run the rest of the notebook to kickoff a job


## Using complete storage URIs instead of azureml:// shortcuts (in case of errors)

If your interactive Spark session or Spark job throws an error while reading or writing to a blob location using the `azureml://` shortcut URI, you may need to use the complete URI path to save. This used to be a known bug within AzureML though it may have been fixed.

The complete URI follows the same pattern as you used to read the raw data: `wasbs://<CONTAINER-NAME>@<STORAGE-ACCOUNT-NAME>.blob.core.windows.net/<PATH-TO-READ/WRITE>`

You can get both the _Container Name_ and the _Storage Account Name_ by doing the following:

1. Click on _Data_ on the left-hand nav-bar of AzureML
1. Click on Datastores tab
1. Click on the Datastore named workspaceblobstore (Default)
1. Highlight in the browser and text-copy the value for `Storage URI` which has the form of https://<STORAGE-ACCOUNT>.blob.core.windows.net/<CONTAINER-NAME>. Every team's values will be different.

Extract the values of <STORAGE-ACCOUNT> and <CONTAINER-NAME> from that URL and use them in your code:

```
workspace_default_storage_account = "<STORAGE-ACCOUNT>"
workspace_default_container = "<CONTAINER-NAME>"

workspace_wasbs_base_url = 
    f"wasbs://{workspace_default_container}@{workspace_default_storage_account}.blob.core.windows.net/"

# save
df.write
    .mode("overwrite")
    .parquet(f"{workspace_wasbs_base_url}<PATH-TO-READ/WRITE>")
```
