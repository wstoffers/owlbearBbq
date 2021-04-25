## Working with Google Cloud Platform (GCP)

### Google Cloud Configuration

Environment configuration began with
```bash
$ gcloud config configurations create owlbear
$ gcloud init --console-only
```
and continued with the following prompt choices
```
[1] Re-initialize this configuration [owlbear] with new settings
[2] Log in with a new account
```
Copied the provided link and pasted it into a browser window, logging in with the desired Google account and following to prompts to receive the verification code. Pasted the verification code into the terminal prompt. Continued with the following prompt choices
```
[2] Create a new project
owlbear-barbecue
```
to finalize the configuration `owlbear` and the project `owlbear-barbecue`. Opened the billing page of [GCP Console](https://console.cloud.google.com) for the project `owlbear-barbecue` to link a billing account.

### Enabling Services

Enabled several services with
```bash
$ gcloud services enable dataproc.googleapis.com
$ gcloud services enable appengine.googleapis.com
$ gcloud services enable cloudscheduler.googleapis.com
$ gcloud services enable storage.googleapis.com
$ gcloud services enable bigquery.googleapis.com
$ gcloud services enable secretmanager.googleapis.com
$ gcloud services enable cloudbuild.googleapis.com
$ gcloud services enable cloudfunctions.googleapis.com
$ gcloud services enable serviceusage.googleapis.com
```

### Identity Access Management/Secrets Manager

Ideally, identity access should be managed carefully. Initial IAM configuration for this project was quick and dirty: simply added one service account to use for GCP Secrets Manager with
```bash
$ gcloud iam service-accounts create owlbear
$ gcloud projects add-iam-policy-binding owlbear-barbecue --member="serviceAccount:owlbear@owlbear-barbecue.iam.gserviceaccount.com" --role="roles/owner"
$ gcloud iam service-accounts keys create owlbearKeyFile.json --iam-account=owlbear@owlbear-barbecue.iam.gserviceaccount.com
$ mv ./owlbearKeyFile.json sandbox/
$ export GOOGLE_APPLICATION_CREDENTIALS='/home/data/sandbox/owlbearKeyFile.json'
$ gcloud secrets create openWeatherSecret --replication-policy="automatic"
$ gcloud secrets versions add openWeatherSecret --data-file="/home/data/sandbox/openWeatherApiKey.temp"
$ gcloud secrets add-iam-policy-binding openWeatherSecret --role roles/secretmanager.secretAccessor --member serviceAccount:owlbear-barbecue@appspot.gserviceaccount.com
```

Also see [more generic Secret Manager configuration example](src/secrets/).

### Google Cloud Storage

Making sure executable permissions are set correctly, executed
```bash
$ ./factor0/src/cloudStorage/createBuckets.sh
```
This can only be executed once, as all GCS buckets reside in the same namespace. Even for a different projct, the bucket names must be changed. If executable permissions are set correctly, bucket size can be determined by executing
```bash
$ ./factor0/src/cloudStorage/getBucketSize.py
```

### Google Cloud Functions

Making sure executable permissions are set correctly, executed
```bash
$ cd factor0/src/cloudFunctions/acquireData/
$ ./deploy.sh
```

### Google Cloud Scheduler

Making sure executable permissions are set correctly, executed
```bash
$ ./factor0/src/cloudScheduler/schedule.sh
```