#!/bin/bash
gcloud app create --region=us-central
gcloud scheduler jobs create http acquireDataJob --schedule "* * * * *" --uri "https://us-central1-owlbear-bbq.cloudfunctions.net/acquireData"
