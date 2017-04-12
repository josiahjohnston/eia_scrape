Scrape data on existing and planned generators in the United States from the
Energy Information Agency to update the catalog for the Switch-WECC database.


TABLE OF CONTENTS
* FILE LAYOUT
* GOALS
* RESOURCES


FILE LAYOUT

pip_requirements.txt is a working list of requirements. It needs to get moved
to a setup.py file.

Most of the code is currently in scrape.py which may later get renamed to
eia860.py. The organization of this package will be fluid while its
functionality gets sorted out. Functions for downloading files in an
archive-safe manner and unzipping files are in utils.py. These should get 
migrated into a package that lives in a subdirectory.

I initially pulled other_dat/*.txt files from pdfs manually, but later found
them in some excel files (I don't remember which ones). Their extraction and
save should get automated, and they should live in the directory with other
auto-extracted files - either downloads or a new directory for intermediate
outputs.


GOALS

Download and archive data in a way that can detect future changes to the
upstream repository (i.e. if the federal datasets are tampered with). We will
use a subset of the data for the moment, but would like to keep the remaining
data available for future work, especially since the upstream datasets could
be removed eventually.

Desired datasets
* EIA-860 which has plant- and unit-level technology, location and various
  characteristics.
* Monthly production by plant (EIA-923)
    - Export historical hydro output for as many years as practical
    - Average monthly output of wind and solar could be useful for 
      reality checks, but isn't top priority
* Monthly fuel inputs by plant (EIA-923)
* Estimated average heat rates based on monthly production & fuel inputs
    - Export to database
* Average capital costs of recently installed generators: these will provide a
  reality check for our cost projections, and can be the left-most datapoint
  in our graphs of cost projections.
    https://www.eia.gov/electricity/generatorcosts/
* O&M costs 2005-2015: http://www.eia.gov/electricity/annual/html/epa_08_04.html
  New construction cost & performance characteristics assumptions, including
  regional capital cost differences:
    http://www.eia.gov/outlooks/aeo/assumptions/pdf/table_8.2.pdf


Summarize select data and export to text files
- Filter out plants that were planned but are now cancelled
- Initially summarize by plant, generator type and vintage
- Eventually aggregate all similar generator within a load zone 
  (could be outside the scope of this script)

Perform quality control on the data, probably with Jupyter notebooks:
- Flag outliers for manual review
- Check that aggregation method is covering all relevant plants and not
  getting thrown off by edge cases of plants that include coal steam turbines,
  gas combined cycle, and gas single cycle units.
- Assess methods of estimating heat rates and comparing to previously estimated
  rates. If we can't do well on edge cases of plants with diverse units, then
  extrapolate to those plants based on similar generation technologies and 
  vintages.
- etc

Write clean code for scraping and data processing so updating the catalogue
for next year's data is easy. If the code is clean and generally useful, we
may be able to recruit outside collaborators to help maintain it. Don't make
this code excessively specialized to Switch if it doesn't need to be.

Push the cleaned and validated data into postgresql

Assign plants to load zones in postgresql

Aggregate similar plants within each load zone in postgresql to reduce the
dataset size for the model. We aren't worried about discrete unit commitment 
for this study at this time.

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
