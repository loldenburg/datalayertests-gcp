Server-Side Data Layer Validation: Google Cloud Platform Setup Guide
==============
This repo complements the server-side data layer testing framework from https://github.com/loldenburg/datalayertest with
code for Google Cloud Functions, Firestore and BigQuery as well as a step-by-step setup guide.

The ideas and benefits of it are presented in this article:

* https://thebounce.io/use-your-server-side-tms-to-qa-your-data-collection-in-the-real-world-7137376ca9a9

# Step-by-step Guide
Most of the steps are also shown in the second part of this video: https://drive.google.com/open?id=1yf3KfnderZeAtrypkVteJzYWkTWgJnlh

## 1. Install/Enable required tools

1. You need a billing-enabled GCP project (
   see: https://cloud.google.com/resource-manager/docs/creating-managing-projects)

### Recommended (for local runs, code changes etc.)

1. Have Git installed on your machine https://git-scm.com/downloads
2. For local runs & tests, you nodeJS, Python 3.9 (3.6-3.10 should also work) and an IDE (recommended: PyCharm)
3. Install the Google Cloud SDK CLI (see: https://cloud.google.com/sdk)
4. Open a PyCharm terminal and authenticate Google Cloud SDK CLI: `gcloud auth login --update-adc`
5. Set your Project ID: `gcloud config set project <your-project-id>`
6. Open `datalayer_tests/log_datalayer_error.py`. PyCharm will ask you (yellow bar on top) to configure a Python
   interpreter and virtual environment. Make sure you add a **new** "local interpreter", don't use an existing one! Do
   NOT inherit global site packages!
7. Install the required Python packages via clicking the PyCharm prompt or by running `pip install -r requirements.txt`
   in the console.

## 2. Fork the GitHub repo

1. Go to https://github.com/loldenburg/datalayertests-gcp
2. Click on "Fork" and fork the project. You now have your own GitHub repository with a copy of the code.
   ![Fork Project](fork-project.png)

## 3. Set up Google Cloud Project components

0. Enable Resource Manager API: `https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com`

1. Go to the **Google Cloud Functions**
    1. Click "create function".
    2. Enable the APIs shown.
    3. When you get to the screen to set up the cloud function, click the "back" button to return to the Cloud Functions
       overview page.

2. Go to **Secret Manager** and enable the Secret Manager API:
    1. In Secret Manager, create a secret called `data-layer-tests-gcf-token` with a secret value of your choice.
       This will be used to authenticate the data layer error log cloud function. It has to be part of your Tealium
       Function
       request to the Cloud Function.
    2. Enable Secret Manager Access for the Service Account that will run our Cloud Function.
        1. Go to IAM
        2. Click on Edit next to the "App Engine default service account" (we will run the Cloud Function with this
           Service Account to keep things simple)
        3. Add the role "Secret Manager Secret Accessor".
        4. If you want to use the __BigQuery__ integration, also give the roles "BigQuery Data Editor" and "BigQuery Job
           User"

3. Go to **Google Cloud Build** and enable the API if not already enabled.
   Cloud Build Triggers will build (=update) your cloud functions every time you push a change to your GitHub
   repository's "main" branch.
    0. Enable `Cloud Functions Developer` and `Secret Manager Secret Accessor` roles for Cloud Build Service Account
       under "Settings".
    1. Select "Triggers" and then "Create Trigger".
    2. Fill the fields as provided in the screenshots below.
        - Name: push-to-build-gcf-data-layer-tests (or whatever you like)
        - Region: global
        - Description: Triggers on commit to main branch and deploys Cloud Function
        - Event: Push to a branch
        - Source: Select "Connect New Repository", then "External Repository", then "GitHub". Then select your
          forked GitHub repository.
        - Branch: ^main$
        - Location: Repository
        - Cloud Build Configuration file location: -> `/cloudbuild.yaml`
        - Advanced: -> Substition variables
            - _ENTRY_POINT: `main_handler`
            - _ERROR_LOG_TOKEN_SECRET_NAME: `data-layer-tests-gcf-token`
            - _ERROR_LOG_TOKEN_SECRET_VERSION: `1` (recommended not to use "latest" because secret is picked up only
              during build -> this way it is explicit, that if you change the secret, you need to rebuild the function (
              with a change in the trigger))
            - _REGION: `us-central1` or a different region if your cloud function should run
              elsewhere. Make sure Cloud Function region = Firestore and BigQuery (if used) region.
            - _SERVICE_NAME: `data-layer-tests-handler`
        - Check "send builds to GitHub" (you can follow the build live in GitHub and see errors there)

          ![img.png](create-trigger-1.png)
          ![img.png](create-trigger-2.png)

    3. Now run the trigger once manually. This will create the cloud functions in your project.
        - You can also trigger a build (bypassing the trigger in the GCF interface) from your local folder with the
          following command:

    ```bash
    gcloud builds submit --substitutions="_SERVICE_NAME=data-layer-tests-handler,_REGION=us-central1,_ENTRY_POINT=main_handler,_ERROR_LOG_TOKEN_SECRET_NAME=data-layer-tests-gcf-token,_ERROR_LOG_TOKEN_SECRET_VERSION=1" --project="<your-project-id>"
    ```

4. Go to **Firestore**
    1. Select Native Mode
    2. Region: nam5 (United States) "multi-region" (or the same region where your cloud function runs)
    3. Click "start collection"
    4. Name the collection "dataLayerErrorLogs"
    5. Delete the document again that Google creates automatically (click the three dots next to it and then "delete")

5. **Make your Cloud Function public**:
    1. Go to [Cloud Functions](https://console.cloud.google.com/functions).
    2. Check the "Authenticated" column for your newly created function. If it does not
       have `"Allow unauthenticated"` written in it, click on the checkbox to the left of the function and then on "
       Permissions".
    3. In the "Permissions" tab, click on "Add member", then "Add Principal".
    4. Add the `allUsers` principal with the `Cloud Functions Invoker`
       role. ![img.png](make-cloud-function-public.png)

6. Go to **Cloud Functions** to get **Trigger URL**
    1. Click on the newly created cloud function "data-layer-tests"
    2. Click on "Trigger"
    3. Copy the Trigger URL

## 4. Configure the code

Open the `datalayer_tests/log_datalayer_error.py` file and follow the "TODO" comments in there (apart from those
regarding BigQuery, which we will tackle later).

## 5. Update URL & Code in Tealium Functions

1. Go into your **Tealium** Function code -> Edit Function -> Global Variables
2. Create a new variable called "urlGCF" and set the value to the Cloud Function Trigger URL we got earlier
3. Create a new variable called "tokenGCF" and set the value to the secret you created in Secret Manager earlier.
4. Go to the JS code and uncomment the rows below `// UNCOMMENT THIS IF YOU HAVE YOUR OWN GCLOUD CONNECTION`
5. Save the function and publish your Tealium CDH profile! Done!

If you want to monitor your data with BigQuery and Data Studio, you can additionally follow the steps below.

## BigQuery & Data Studio integration

### BigQuery

1. In your GCP project, go to BigQuery -> SQL Workspace.
2. Click on the 3 dots next to your project ID and select "create dataset".
3. Name the dataset `datalayer_errors`.
4. Choose a region that matches the region of your Cloud Function (default "us-central1")
5. Click into the SQL query editor tab on the right.
6. Create the table where the data layer errors will be held by copying and running the following SQL query,
   replacing `{{your_project_id}}` by your GCP project ID:

```sql
#@formatter:off
-- copy query from here...
CREATE TABLE IF NOT EXISTS
  `{{your_project_id}}.datalayer_errors.datalayer_error_logs` ( 
    event_id STRING NOT NULL,
    event_name STRING NOT NULL,
    error_types STRING,
    error_vars STRING,
    logged_at TIMESTAMP NOT NULL,
    url_full STRING,
    user_id STRING,
    tealium_profile STRING 
    )
-- ...to here
#@formatter: on
```
7. Go to IAM. Select the "App Engine default service account" and confirm that this account has "BigQuery Data Editor" and "BigQuery Job User" permissions. #todo check if that is correct!
8. Finally, go into your code editor and open the file `data-layer-tests-gcp/datalayer_error_log.py`. (Make sure you have HTTP Proxies like **Fiddler off** now!)
9. Set env variables to run your code locally (guide for PyCharm). 
   1. Set a breakpoint at the start of the code and then click on Debug above the code editor.
   2. When the code pauses at the breakpoint, click on the dropdown next to the green "play" button and select "Edit Configurations".
   3. Add "GCP_PROJECT_ID" with the value of your GCP project id ![img.png](edit-env-vars-pycharm.png)
10. Change the row `big_query_enabled = False` to `big_query_enabled = True` and push to the main branch of your GitHub repository. 
11. After the build, your cloud function will now write all your data layer errors to BigQuery.

### Data Studio

Copy this example (https://datastudio.google.com/reporting/93f7383a-10e4-4340-a278-931cee27d7fa) and connect it to your own BigQuery data: 
1. Resources -> Manage Added Data Sources 
2. Change to your GCP Project's BigQuery table
![img.png](data-studio-example.png)