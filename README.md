Portions of plots might not be visible in this README.<br/>
[GitHub Pages site for this project](https://wstoffers.github.io/owlbearBbq/) is under construction.

### Road Map
- [x] GCP Secret Manager setup
- [x] GCP Cloud Scheduler + GCP Cloud Function calling REST API
- [x] IOT thermocouple install
- [ ] GCP Cloud Scheduler + GCP Cloud Function calling SOAP API (early May '21)
- [x] IOT weather station install
- [x] Population of GCP Cloud Storage data lake
- [ ] GCP Dataproc PySpark transform to Parquet w/ compaction (early May '21)
- [ ] GCP Dataflow to Firestore pipeline for anomoly detection and mobile notification (mid May '21)
- [ ] GCP Cloud SQL loading for idempotence (late May '21)
- [ ] Loading of GCP BigQuery data warehouse for ad hoc analysis (late May '21)
- [ ] Bayesian inference (early June '21)
- [ ] Refactor to AWS/Snowflake (early June '21)
- [ ] Pytest additions (ongoing)

The Texas Crutch is a well-known barbecue technique that pitmasters use to improve resource usage during a long cook. In a time where 86% of restaurants are reporting a drop in profit margins over the last year, resource management is paramount[[1]](#references). But is wrapping a brisket the only way to increase return on investment as a restaurant owner? And is the real Texas Crutch actually the weather?

Whether or not Austin has a meteorological advantage, protecting a smoker from the elements is commonly thought of as helpful. Dry, still air is a good thermal insulator. But merely introducing convection changes the game, let alone precipitation. Water has a higher specific heat capacity and a higher heat transfer coefficient than air, making it an effective coolant.

It follows that chefs, trying to maintain as constant a temperature as possible over half a day, seek to remove these competing factors. But if a roof alone costs $10,000 does it ever pay for itself in fuel saved, reduced waste, or improved quality? Smoking a brisket will always be an art, but data-driven methods can tease out relationships that allow pitmasters to focus on their craft.

## Follow the Data

With Austin's Franklin Barbecue already on his resume, Chef Karl Fallenius opened Denver's Owlbear Barbecue on May 9, 2019[[2]](#references). Since then, his windy corner has been an olfactory temptation for passers-by - rooted in Central Texas techniques, but with a Denver twist. It is this unique combination that makes Owlbear a valuable study. Regionality in smoked brisket is often debated, with the focus on wood or ingredient sourcing. This examination, however, is focused on regional weather.

#### Figure 1: Flow of Data
![Flow of Data](https://raw.githubusercontent.com/wstoffers/owlbearBbq/main/factor0/images/flowOfData.png)

### Data Ingestion

- Purpose of automated pipeline is 24/7 ingestion availability, matching the cook
    - Scheduled Cloud Function API calls to OpenWeatherMap API[[3]](#references) & API in Azure
    - Scheduled Spark Cluster Workflow Templating<sup>†</sup>
- Initial load
- API calls instead of subscribing to publisher
- Security: Keys secured in GCP Secret Manager, enabling ephemerality
- Stem the tide of streaming data
    - Cook chamber temperature data streams in near real time<sup>‡</sup>
    - Gradually step down in rate since real time delivery is costly & not needed