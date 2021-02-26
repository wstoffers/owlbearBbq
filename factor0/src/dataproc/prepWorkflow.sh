#!/bin/bash
gcloud dataproc workflow-templates create initial-transform-template --region=us-central1 --project=owlbear-bbq

gcloud dataproc workflow-templates set-managed-cluster initial-transform-template --cluster-name=initialtransform --region=us-central1 --master-machine-type=e2-standard-2 --worker-machine-type=e2-standard-2 --num-workers=2 --project=owlbear-bbq

gcloud dataproc workflow-templates add-job pyspark jsonTransformParquet.py --step-id=read-json --workflow-template=initial-transform-template --region=us-central1 --project=owlbear-bbq


