Scrape project and production data on existing and planned generators in the
United States from the Energy Information Agency for several years. Data are
processed and uploaded to a Postgresql database.


TABLE OF CONTENTS
* FILE LAYOUT
* GOALS
* RESOURCES


GOALS

To provide useful generation project data for power system modeling. In
particular, to format this data for its use in the Switch power system
planning model <https://github.com/switch-model/switch>.

Download and archive data in a way that can detect future changes to the
upstream repository (i.e. if the federal datasets are tampered with). We will
use a subset of the data for the moment, but would like to keep the remaining
data available for future work, especially since the upstream datasets could
be removed eventually.

Code for data scraping and processing is ment to be clean, so that updating the
catalogue as new data is released each year is easy. The code tries to be clean
and generally useful, so recruiting outside collaborators to help maintain it
is possible.


FILE LAYOUT

pip_requirements.txt is a working list of requirements. It needs to get moved
to a setup.py file.

The scraping code is currently in scrape.py which may later get re-organized as
a package. Functions for downloading files in an archive-safe manner and
unzipping files are in utils.py. Functions to interact with the Postgresql
database are in database_interface.py. All these should get migrated into a
package that lives in a subdirectory.

The codes located in other_dat/* were manually extracted from the latest
"Layout" Excel workbook from the EIA860 form. Their extraction and save should
get automated, and they should live in the directory with other auto-extracted
files - either downloads or a new directory for intermediate outputs.

The average heat rates located in other_dat/* were manually extracted from the
EIA website.

The following resulting datasets contain data suitable for general use in power
system analysis and modeling:

* generation_projects_YYYY.tab:
  Unit-level characteristics sourced from the EIA 860 form. Turbines belonging
  to the same combined cycle are lumped together. Units are aggregated by plant,
  technology, energy source and vintage. This is usually the case for plants
  with several identical units, such as a motor facility.
  - Technology, location, capacity, vintage, energy source, and other key data
  - This outputs are not ment to be used for unit commitment modeling
  - All existing plants and all those under construction are processed. Plants
    in planning stages are only included if they have initiated their regulatory
    approval process.

* historic_heat_rates_(NARROW/WIDE).tab:
  Monthly generation data for thermal projects sourced from the EIA 923 form
  and crossed with generation project data from the EIA 860 form. The EIA 923
  form reports data on a plant-level basis, so generation data is also
  calculated in that basis. All coal types are treated indistinctly, but
  generation data is reported for all fuels if a plant use multiple energy
  sources. The following data is provided for each plant and fuel:
  - Monthly net electricity production
  - Monthly capacity factor
  - Monthly heat rate
  - Fraction of electricity produced by each of the plant's fuels
  - Singles out the second best monthly heat rate calculated
  Missing plants from either the EIA860 or the EIA923 forms are printed out to
  the file incomplete_data_thermal_YYYY.csv
  Plants that use a secondary fuel to generate more than 5% of their electricity
  are also printed to multi_fuel_heat_rates.tab
  Plants with consistently negative heat rates are printed out to
  negative_heat_rate_outputs.tab and are removed from the historic dataset

* historic_hydro_capacity_factors_(NARROW/WIDE).tab:
  Monthly generation data for hydro projects sourced from the EIA 923 form
  and crossed with generation project data from the EIA 860 form. The following
  data is provided for each plant:
  - Monthly net electricity production
  - Monthly electricity consumption (relevant for pumped hydro plants)
  - Monthly capacity factor (calculated on the basis of electricity generated)
  Missing plants from either the EIA860 or the EIA923 forms are printed out to
  the file incomplete_data_hydro_YYYY.csv


Quality control:
  - Mismatches between the plants present in the EIA-860 and EIA-923 forms are
    registered in csv files, and a summary of the incomplete information is
    printed to the console
  - Historical hydro capacity factors and heat rates are printed alongside
    other relevant data, so QA/QC can be done by visual inspection as well
  - To Do: Flag outliers for manual review
  - To Do: Maybe use Jupyter Notebooks for manual filtering

* existing_generation_projects_YYYY.tab, new_generation_projects_YYYY.tab,
  uprates_to_generation_projects_YYYY.tab:
  These datasets result from crossing project data with heat rate data, as well
  as filtering by NERC region.
  - Plants with better heat rates than the best historical records found online
    are ignored and assigned an average heat rate per technology, since it is
    assumed that reporting errors ocurred.
  - The top and bottom .5% of heat rates are also ignored, since they contain
    unrealistic values. These heat rates get replaced by the heat rate at the
    top and bottom .5 percentile, respectively.
  - Plants without heat rate data (such as plants under construction or with
    missing information in the EIA923 form) are assigned the average heat rate
    of plants with the same technology, energy source and vintage, considering
    a 4-year window.

* heat_rate_distributions.pdf:
  Histograms showing the distribution of heat rate values per technology and
  energy source.


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
    Average heat rates for benchmarking.
