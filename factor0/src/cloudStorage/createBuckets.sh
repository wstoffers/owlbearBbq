#!/bin/bash
gsutil mb -p owlbear-barbecue -l US-CENTRAL1 -b on gs://wstoffers-galvanize-owlbear-bbq-data-lake-raw
gsutil mb -p owlbear-barbecue -l US-CENTRAL1 -b on gs://wstoffers-galvanize-owlbear-bbq-data-lake-transformed
