#!/bin/bash
gsutil mb -p owlbear-bbq -l US-CENTRAL1 -b on gs://wstoffers-galvanize-owlbear-data-lake-raw
gsutil mb -p owlbear-bbq -l US-CENTRAL1 -b on gs://wstoffers-galvanize-owlbear-data-lake-transformed
