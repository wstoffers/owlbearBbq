#!/bin/bash
files=(../data/sandbox/owlbear2021-02-20-11.58.55.781392-Sat.json
       ../data/sandbox/franklin2021-02-20-11.58.55.781392-Sat.json
       ../data/sandbox/owlbear2021-02-20-19.42.51.465358-Sat.json
       ../data/sandbox/franklin2021-02-20-19.42.51.465358-Sat.json
       ../data/sandbox/owlbear2021-02-20-21.13.46.052735-Sat.json
       ../data/sandbox/franklin2021-02-20-21.13.46.052735-Sat.json
       ../data/sandbox/owlbear2021-02-20-22.22.49.704514-Sat.json
       ../data/sandbox/franklin2021-02-20-22.22.49.704514-Sat.json
       ../data/sandbox/owlbear2021-02-20-22.25.47.556636-Sat.json
       ../data/sandbox/franklin2021-02-20-22.25.47.556636-Sat.json
)
for file in "${files[@]}"; do
    gsutil cp $file gs://wstoffers-galvanize-owlbear-data-lake-raw/
done
