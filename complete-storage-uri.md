## Using complete storage URIs instead of azureml:// shortcuts (in case of errors)

If your Spark session throws an error while reading or writing to a blob location using the azureml:// shortcut URI, you may need to use the complete URI path to save. This is a known bug within AzureML.

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

