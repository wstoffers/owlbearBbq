# The Texas Crutch

The Texas Crutch is a well-known barbecue technique that pitmasters use to improve resource usage during a long cook. In a time where 86% of restaurants are reporting a drop in profit margins over the last year, resource management is paramount[[1]](#references). But is wrapping a brisket the only way to increase return on investment as a restaurant owner? And is the real Texas Crutch actually the weather?

Whether or not Austin has a meteorological advantage, protecting a smoker from the elements is commonly thought of as helpful. Dry, still air is a good thermal insulator. But merely introducing convection changes the game, let alone precipitation. Water has a higher specific heat capacity and a higher heat transfer coefficient than air, making it an effective coolant.

It follows that chefs, trying to maintain as constant a temperature as possible over half a day, seek to remove these competing factors. But if a roof alone costs $10,000 does it ever pay for itself in fuel saved, reduced waste, or improved quality? Smoking a brisket will always be an art, but data-driven methods can tease out relationships that allow pitmasters to focus on their craft.

## Follow the Data

With Austin's Franklin Barbecue already on his resume, Chef Karl Fallenius opened Denver's Owlbear Barbecue on May 9, 2019[[2]](#references). 

#### Figure 1
![Flow of Data](factor0/images/flowOfData.png)

### Data Ingestion

- Initial load
- API calls[[3]](#references) instead of subscribing to publisher
- Security
- Stem the tide of streaming data

#### Figure 2
![Sample Thermocouple Data](factor0/images/thermoworksPlot2021.02.25.21.37.png)

<sup>†</sup>While the cloud thermocouple was installed on 2/24/21 and is gathering temperature data, this portion of the pipeline has been postponed and will be integrated into v0.2.

### Data Transformation

- Mixed file types
- Cleaning
- Preparation for analysis

#### Compaction

- File read time issues
- Computing time v. Engineering time

### Data Insight

- Coming soon to a theater near you!

## References
1. [National Restaurant Association Report](https://restaurant.org/downloads/pdfs/advocacy/covid-19-restaurant-impact-survey-v-state-results)
1. [Owlbear Opens Brick-and-Mortar](https://www.denverpost.com/2019/05/09/owlbear-barbecue-restaurant-open-denver/)
1. [Open Weather Map API](http://api.openweathermap.org/)

[(top)](#the-texas-crutch)