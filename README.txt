Scrape project and production data on existing and planned generators in the
United States from the Energy Information Agency for several years.


TABLE OF CONTENTS
* FILE LAYOUT
* GOALS
* RESOURCES


FILE LAYOUT

pip_requirements.txt is a working list of requirements. It needs to get moved
to a setup.py file.

Most of the code is currently in scrape.py which may later get re-organized as
a package. Functions for downloading files in an archive-safe manner and
unzipping files are in utils.py. These should get migrated into a package that
lives in a subdirectory.

The codes located in other_dat/* were manually extracted from the latest
"Layout" Excel workbook from the EIA860 form. Their extraction and save should
get automated, and they should live in the directory with other auto-extracted
files - either downloads or a new directory for intermediate outputs.

The average heat rates located in other_dat/* were manually extracted from the
EIA website.


GOALS

To provide useful generation project data for power system modeling.

Download and archive data in a way that can detect future changes to the
upstream repository (i.e. if the federal datasets are tampered with). We will
use a subset of the data for the moment, but would like to keep the remaining
data available for future work, especially since the upstream datasets could
be removed eventually.

Code for data scraping and processing is ment to be clean, so that updating the catalogue as new data is released each year is easy. The code tries to be clean and generally useful, so recruiting outside collaborators to help maintain it is possible.

Desired datasets:
* Plant- and unit-level characteristics (EIA-860)
  - Technology, location, capacity, and other key characteristics
  - Maximum level of plant aggregation. This outputs are not ment to be used
  for unit commitment modeling, and consumption/generation data from the EIA923
  form is specified per plant and prime mover, NOT per unit
* Monthly net electricity production and capacity factor (EIA-923)
  - Hydro plants: Historical output for as many years as practical
  - Wind and solar plants: Historical output could be useful for reality
    checks, but isn't top priority
* Monthly heat rate, net electricity production, fuel consumption and capacity
  factor (EIA-923)
  - Thermal plants: Historical output for as many years as practical
* Average capital costs of recently installed generators: these will provide a
  reality check for our cost projections, and can be the left-most datapoint
  in our graphs of cost projections.
    https://www.eia.gov/electricity/generatorcosts/
* O&M costs 2005-2015:
  http://www.eia.gov/electricity/annual/html/epa_08_04.html
* New construction cost & performance characteristics assumptions, including
  regional capital cost differences:
    http://www.eia.gov/outlooks/aeo/assumptions/pdf/table_8.2.pdf


Select data is summarized and exported to tab separated files:
  - Plant-level data for each generation project is first aggregated by plant,
    technology, energy source, and vintage. There are two other aggregations:
    --By plant and technology; for consistency with consumption/generation data
    --Gas and steam turbines belonging to combined cycle plants
  - Units' minimum stable generation level, time from cold shutdown to full
    load, geographical coordinates, and other specific features are only
    extracted for the most recent year.
  - Only proposed plants that have initiated construction or at least have
    their regulatory approvement pending are be considered.
  - Further aggregation may be achieved for unit data by modifying the
    aggregation lists, though it could be better to upload data with the
    current aggregation level and perform further operations directly in the DB

Quality control:
  - Mismatches between the plants present in the EIA-860 and EIA-923 forms are
    registered in csv files, and a summary of the incomplete information is
    printed to the console
  - Historical hydro capacity factors and heat rates are printed alongside
    other relevant data, so QA/QC can be done by visual inspection as well
  - To Do: Flag outliers for manual review
  - To Do: Maybe use Jupyter Notebooks for manual filtering

RESOURCES

Please keep updating & expanding this list as you explore available data.

EIA Electricity Data
https://www.eia.gov/electricity/data.cfm

EIA-860 - Catalog of existing & planned generation
https://www.eia.gov/electricity/data/eia860/
    generator-level specific information about existing and planned generators
    and associated environmental equipment at electric power plants with 1
    megawatt or greater of combined nameplate capacity. 
    Static data (zip files) and documentation

EIA-923 - Input and output of existing generators
https://www.eia.gov/electricity/data/eia923/
    detailed electric power data -- monthly and annually -- on electricity
    generation, fuel consumption, fossil fuel stocks, and receipts at the
    power plant and prime mover level. 
    Static data (zip files) and documentation

EIA OPEN DATA API
https://www.eia.gov/opendata/?category=0
This could be used for everything (including EIA-860 datasets).
We need to assess whether this is easier or harder to use than static datasets
from zip files.
* API Query Browser
  https://www.eia.gov/opendata/qb.php
* The bulk download facility may be better for archiving
  https://www.eia.gov/opendata/bulkfiles.php
* If we use their URL-based API, we should still use the functionality of utils
  to cache download results in an archive-safe manner.

EIA Electricity Data Browser
https://www.eia.gov/electricity/data/browser/
* Interactive graphical website for browsing their entire data portal.
* Can potentially use to help construct API queries, but another one of their 
  tools may be more useful for that.

EIA Average Tested Heat Rates by Prime Mover and Energy Source, 2007 - 2015
https://www.eia.gov/electricity/annual/html/epa_08_02.html
    average heat rates for benchmarking
