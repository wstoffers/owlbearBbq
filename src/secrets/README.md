## Configure Google Secret Manager

### Using Google Cloud SDK

Use a secret manager to protect API keys, so secrets can be ephemeral.
```bash
$ gcloud iam service-accounts create serviceAccountName
$ gcloud projects add-iam-policy-binding projectName --member="serviceAccount:serviceAccountName@projectName.iam.gserviceaccount.com" --role="roles/owner"
$ gcloud iam service-accounts keys create nameOfKeyFile.json --iam-account=serviceAccountName@projectName.iam.gserviceaccount.com
```
Do not version control `nameOfKeyFile.json` JSON secret; exclude from Git with `.gitignore` file.