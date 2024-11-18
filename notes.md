Estimated allowance is $300 per team


## Log into your team's Azure Machine Learning (AzureML) Workspace

Navigate to https://ml.azure.com and login with your GU credentials
Click workspaces
You should see your team's workspace named project-group-##


## Your compute instance

Compute instances were created for each team member. These compute instances are individual, and only the named person can use them. You need to use the compute instances for any github operation.



### Do this the first time you use a new compute instance

The first time you start your Compute Insance, you should run the following commands from the terminal of that machine. Change the values in <> to your own. The netid is without `@georgetown.edu`. You only need to run these commands once (but if you add another compute instance, you'll need to do it again.)

`az upgrade`
`az extension remove -n azure-cli-ml`
`az extension remove -n ml`
`az extension add -n ml -y`
`git config --global --add safe.directory "/home/azureuser/cloudfiles/code/Users/<NETID>/*"`
`git config --global user.email "<YOUR-EMAIL>"`
`git config --global user.name "<YOUR-NAME>"`

## Data location

The Azure Blob location that has the project data is: `wasbs://reddit-project@dsan6000fall2024.blob.core.windows.net/<DIRECTORY>/`.

- We've added the Reddit data for this year's project which spans the June-2023 to July-2024 in the `202306-202407` directory
- We've also added data from prior years that spans Jan-2021 to March-2023 in the `202101-202303` directory

Within each dataset directory the structure is similar. There is a `comments` and `submissions` subdirectory, and then the data is partitioned by year and month.

- The `202306-202407` dataset uses the `yyyy=####/mm=##` partitioning schema
- The `202101-202303` dataset uses the `year=####/month=##` partitioning schema

**Note:** There are differences in the both datasets with the partitioning schema names, the actual individual parquet file names, and the field names. If you do end up using both sets and want to stack them together (noting there is a three month gap), you'll have to process them individually and generate a common schema.
d2c-450d-93b7-96eeb3699b22

## Clone the project repo and collaborate within AzureML

- Every team member should clone the project repo to this path: 



## Save intermediate datasets

You



## Interactive Spark


## Running a job