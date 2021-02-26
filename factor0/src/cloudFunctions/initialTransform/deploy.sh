#!/bin/bash
gcloud functions deploy initialTransform --region us-central1 --runtime python38 --memory=128MB --max-instances=1 --trigger-http --allow-unauthenticated
