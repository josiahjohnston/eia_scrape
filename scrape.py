# Copyright 2017. All rights reserved. See AUTHORS.txt
# Licensed under the Apache License, Version 2.0 which is in LICENSE.txt
"""
Scrape data on existing and planned generators in the United States from the 
Energy Information Agency's EIA860 and EIA923 forms (and their older versions).

Enables sequential aggregation of generator data by multiple criteria and
filtering projects of specific NERC Regions.

Extracts monthly capacity factors for each hydroelectric generation plant.

All data is scrapped and parsed from 2004 onwards.

To Do:
Extract historic heat rates.
More error checks.
More QA/QC on output.
Set Prime Mover of aggregated combined cycle plants to 'CC'.
Filter proposed plants by Status.
Include Status column in generation projects' output sheet.
Calculate hydro outputs previous to 2004 with nameplate capacities of that year,
but first check that uprating is not significant for hydro plants.
Use packages for specific functions instead of writing custom code.
Print output sheet in a frendlier order (Plant Id, Plant Name, ..., optionals)
Move WECC filtering snippets to a separate function and allow filtering by other
Regions or not at all.
Drop Unit Code column on output sheets.
Add 'EIA' prefix on Code columns on output sheets.
Do a reality check on historic hydro outputs and thermal heat rates.
Re-organize specific snippets of code into functions.
Write a report with assumptions taken and observations noticed during the writing
of these scripts and data analysis.

Assumptions:
Only single fuels are considered.
Ignoring uprates and downrates.
Ignore summer and winter capacities.
Some data is only extracted for the last year.
If more than 50% of a county's area is in WECC, then its plants are included.
Taking the 'max' value of each column that is not summed in the aggregation process
of generation projects (the most relevant effect of this is taking the maximum
time from cold shutdown to full load for several units aggregated into a plant).

"""

import csv
import os
import numpy as np
import pandas as pd

# Update the reference to the utils module after this becomes a package
from utils import download_file, download_metadata_fields, unzip, connect_to_db_and_run_query

unzip_directory = 'downloads'
other_data_directory = 'other_dat'
outputs_directory = 'processed_data'
log_path = os.path.join(unzip_directory, 'download_log.csv')
REUSE_PRIOR_DOWNLOADS = True
CLEAR_PRIOR_OUTPUTS = True
start_year, end_year = 2004,2015
fuel_prime_movers = ['ST','GT','IC','CA','CT','CS','CC']
wecc_states = ['WA','OR','CA','AZ','NV','NM','UT','ID','MT','WY','CO','TX']
gen_relevant_data = ['Plant Code', 'Plant Name', 'Generator Id',
                    'Prime Mover', 'Unit Code', 'Nameplate Capacity (MW)',
                    'Operating Year', 'Planned Retirement Year',
                    'Energy Source', 'Operational Status','County']
gen_data_to_be_summed = ['Nameplate Capacity (MW)']
gen_aggregation_lists = [
                            ['Plant Code','Unit Code'],
                            ['Plant Code', 'Prime Mover', 'Energy Source',
                            'Operating Year']
                        ]
gen_relevant_data_for_last_year = ['Time From Cold Shutdown To Full Load',
                        'Latitude','Longitude','Balancing Authority Name',
                        'Grid Voltage (kV)', 'Carbon Capture Technology']
gen_data_to_be_summed_for_last_year = ['Minimum Load (MW)']
months = {
    'January':31,
    'February':28,
    'March':31,
    'April':30,
    'May':31,
    'June':30,
    'July':31,
    'August':31,
    'September':30,
    'October':31,
    'November':30,
    'December':31}
mispelled_counties = [
    'Claveras'
    ]


def uniformize_names(df):
    df.columns = [str(col).title().replace('_',' ') for col in df.columns]
    df.columns = [str(col).replace('\n',' ').replace(
                    '(Mw)','(MW)').replace('(Kv)','(kV)') for col in df.columns]
    df.rename(columns={
        'Sector':'Sector Number',
        'Carboncapture': 'Carbon Capture Technology',
        'Carbon Capture Technology?':'Carbon Capture Technology',
        'Nameplate':'Nameplate Capacity (MW)',
        'Plant Id':'Plant Code',
        'Reported Prime Mover':'Prime Mover',
        'Reported Fuel Type Code':'Energy Source',
        'Energy Source 1':'Energy Source',
        'Plntname':'Plant Name',
        'Plntcode':'Plant Code',
        'Gencode':'Generator Id',
        'Primemover':'Prime Mover',
        'Current Year':'Operating Year',
        'Utilcode':'Utility Id',
        'Nerc':'Nerc Region',
        'Insvyear':'Operating Year',
        'Retireyear':'Planned Retirement Year',
        'Cntyname':'County',
        'Proposed Nameplate':'Nameplate Capacity (MW)'
        }, inplace=True)
    return df


def main():
    for directory in (unzip_directory, other_data_directory, outputs_directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    if CLEAR_PRIOR_OUTPUTS:
        for f in os.listdir(outputs_directory):
            os.remove(os.path.join(outputs_directory,f))

    zip_file_list = scrape_eia860()
    unzip(zip_file_list)
    parse_eia860_dat([os.path.splitext(f)[0] for f in zip_file_list])

    zip_file_list = scrape_eia923()
    unzip(zip_file_list)
    parse_eia923_dat([os.path.splitext(f)[0] for f in zip_file_list])


def scrape_eia860():
    if not os.path.exists(unzip_directory):
        os.makedirs(unzip_directory)
    log_dat = []
    file_list = ['eia860{}.zip'.format(year) for year in range(start_year, end_year+1)]
    for filename in file_list:
        local_path = os.path.join(unzip_directory, filename)
        if REUSE_PRIOR_DOWNLOADS and os.path.isfile(local_path):
            print "Skipping " + filename + " because it was already downloaded."
            continue
        print "Downloading " + local_path
        url = 'http://www.eia.gov/electricity/data/eia860/xls/' + filename
        meta_data = download_file(url, local_path)
        log_dat.append(meta_data)

    # Only write the log file header if we are starting a new log
    write_log_header = not os.path.isfile(log_path)
    with open(log_path, 'ab') as logfile:
        logwriter = csv.writer(logfile, delimiter='\t',
                               quotechar="'", quoting=csv.QUOTE_MINIMAL)
        if write_log_header:
            logwriter.writerow(download_metadata_fields)
        logwriter.writerows(log_dat)
    
    return [os.path.join(unzip_directory, f) for f in file_list]


def scrape_eia923():
    if not os.path.exists(unzip_directory):
        os.makedirs(unzip_directory)
    log_dat = []
    file_list = ['f923_{}.zip'.format(year) if year >= 2008
                    else 'f906920_{}.zip'.format(year)
                        for year in range(start_year, end_year+1)]
    for filename in file_list:
        local_path = os.path.join(unzip_directory, filename)
        if REUSE_PRIOR_DOWNLOADS and os.path.isfile(local_path):
            print "Skipping " + filename + " because it was already downloaded."
            continue
        print "Downloading " + local_path
        url = 'https://www.eia.gov/electricity/data/eia923/xls/' + filename
        meta_data = download_file(url, local_path)
        log_dat.append(meta_data)

    # Only write the log file header if we are starting a new log
    write_log_header = not os.path.isfile(log_path)
    with open(log_path, 'ab') as logfile:
        logwriter = csv.writer(logfile, delimiter='\t',
                               quotechar="'", quoting=csv.QUOTE_MINIMAL)
        if write_log_header:
            logwriter.writerow(download_metadata_fields)
        logwriter.writerows(log_dat)
    
    return [os.path.join(unzip_directory, f) for f in file_list]


def parse_eia923_dat(directory_list):
    for directory in directory_list:
        year = int(directory[-4:])
        print "============================="
        print "Processing data for year {}.".format(year)
        # Name of the relevant spreadsheet is not consistent throughout years
        # Read largest file in the directory instead of looking by name
        largest_file = max([os.path.join(directory, f)
            for f in os.listdir(directory)], key=os.path.getsize)
        if year >= 2011:
            rows_to_skip = 5
        else:
            rows_to_skip = 7
        generation = uniformize_names(pd.read_excel(largest_file,
            sheetname='Page 1 Generation and Fuel Data', skiprows=rows_to_skip))
        print "Read in data for {} generation plants in the US.".format(len(generation))

        # Filter to units in the WECC region
        generation = generation[generation['Nerc Region']=='WECC']
        generation.reset_index(drop=True, inplace=True)
        hydro_generation = generation[generation['Energy Source']=='WAT']
        fuel_based_generation = generation[generation['Prime Mover'].isin(fuel_prime_movers)]
        print "Filtered to {} generation plants in the WECC Region.".format(len(generation))
        print "\tHydro projects:{}".format(len(hydro_generation))
        print "\tFuel based projects:{}".format(len(fuel_based_generation))
        print "\tOther projects:{}".format(len(generation) - len(fuel_based_generation) - len(hydro_generation))

        generation_projects = pd.read_csv(
            os.path.join(outputs_directory,'generation_projects_{}.tab').format(year),
            sep='\t')
        hydro_gen_projects = generation_projects[
            (generation_projects['Operational Status']=='Operable') &
            (generation_projects['Energy Source']=='WAT')]
        fuel_based_gen_projects = generation_projects[
            (generation_projects['Operational Status']=='Operable') &
            (generation_projects['Prime Mover'].isin(fuel_prime_movers))]

        # Hydro projects are aggregated by plant and prime mover
        gb = hydro_gen_projects.groupby(['Plant Code','Prime Mover'])
        hydro_gen_projects = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                                        for datum in gen_relevant_data})
        print "Read existing hydro projects in form EIA860 and aggregated into {} plants.".format(len(hydro_gen_projects))

        # Fuel projects are aggregated by plant and prime mover
        gb = fuel_based_gen_projects.groupby(['Plant Code','Prime Mover'])
        fuel_based_gen_projects = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                                        for datum in gen_relevant_data})

        # Cross-check data
        if hydro_gen_projects['Plant Code'].isin(hydro_generation['Plant Code']).value_counts()[True] == len(hydro_gen_projects):
            print "All hydro projects registered in the EIA860 form that have data in the EIA923 form."
        else:
            print "{} hydro projects registered in the EIA860 form do not have data in the EIA923 form:".format(
                hydro_gen_projects['Plant Code'].isin(hydro_generation['Plant Code']).value_counts()[False])
            for plant in hydro_gen_projects[~hydro_gen_projects['Plant Code'].isin(hydro_generation['Plant Code'])]['Plant Name']:
                print "\t{}: {} MW of capacity".format(plant,hydro_gen_projects[hydro_gen_projects['Plant Name']==plant]['Nameplate Capacity (MW)'].iloc[0])
        if hydro_generation['Plant Code'].isin(hydro_gen_projects['Plant Code']).value_counts()[True] == len(hydro_generation):
            print "All hydro projects with data in the EIA923 form exist in the EIA860 registry."
        else:
            print "{} hydro projects with data in the EIA923 form do not exist in the EIA860 registry:".format(
                hydro_generation['Plant Code'].isin(hydro_gen_projects['Plant Code']).value_counts()[False])
            for plant in hydro_generation[~hydro_generation['Plant Code'].isin(hydro_gen_projects['Plant Code'])]['Plant Name']:
                print "\t{}: {} MWh of generation".format(plant, hydro_generation[hydro_generation['Plant Name']==plant].filter(regex=r'(?i)netgen').sum(axis=1).sum())

        if fuel_based_gen_projects['Plant Code'].isin(fuel_based_generation['Plant Code']).value_counts()[True] == len(fuel_based_gen_projects):
            print "\nAll thermal projects registered in the EIA860 form that have data in the EIA923 form."
        else:
            print "{} thermal projects registered in the EIA860 form do not have data in the EIA923 form:".format(
                fuel_based_gen_projects['Plant Code'].isin(fuel_based_generation['Plant Code']).value_counts()[False])
            for plant in fuel_based_gen_projects[~fuel_based_gen_projects['Plant Code'].isin(fuel_based_generation['Plant Code'])]['Plant Name']:
                print "\t{}: {} MW of capacity".format(plant,fuel_based_gen_projects[fuel_based_gen_projects['Plant Name']==plant]['Nameplate Capacity (MW)'].iloc[0])
        if fuel_based_generation['Plant Code'].isin(fuel_based_gen_projects['Plant Code']).value_counts()[True] == len(fuel_based_generation):
            print "All thermal projects with data in the EIA923 form exist in the EIA860 registry."
        else:
            print "{} thermal projects with data in the EIA923 form do not exist in the EIA860 registry:".format(
                fuel_based_generation['Plant Code'].isin(fuel_based_gen_projects['Plant Code']).value_counts()[False])
            for plant in fuel_based_generation[~fuel_based_generation['Plant Code'].isin(fuel_based_gen_projects['Plant Code'])]['Plant Name']:
                print "\t{}: {} MWh of generation".format(plant, fuel_based_generation[fuel_based_generation['Plant Name']==plant].filter(regex=r'(?i)netgen').sum(axis=1).sum())

        # Save hydro profiles
        hydro_outputs=pd.concat([
            hydro_generation[['Plant Code','Plant Name','Prime Mover']],
            hydro_generation.filter(regex=r'(?i)netgen')
            ], axis=1).replace('.', 0)
        hydro_outputs.loc[:,'Year']=year
        hydro_outputs=pd.merge(hydro_outputs, hydro_gen_projects[['Plant Code','Prime Mover','Nameplate Capacity (MW)']],
            on=['Plant Code','Prime Mover'], suffixes=('',''))
        for i,m in enumerate(months):
            hydro_outputs.rename(columns={hydro_outputs.columns[3+i]:i+1}, inplace=True)
            hydro_outputs.loc[:,i+1] = hydro_outputs.loc[:,i+1].div(months[m]*24*hydro_outputs['Nameplate Capacity (MW)'])
            
        # Save heat rate profiles
        heat_rate_outputs=pd.concat([
            fuel_based_generation[['Plant Code','Plant Name','Prime Mover','Energy Source']],
            fuel_based_generation.filter(regex=r'(?i)elec[_\s]mmbtu'),
            fuel_based_generation.filter(regex=r'(?i)netgen')
            ], axis=1).replace('.', 0)
        heat_rate_outputs.loc[:,'Year']=year
        heat_rate_outputs=pd.merge(heat_rate_outputs, fuel_based_gen_projects[['Plant Code','Prime Mover','Nameplate Capacity (MW)']],
            on=['Plant Code','Prime Mover'], suffixes=('',''))
        for i,m in enumerate(months):
            heat_rate_outputs.rename(columns={heat_rate_outputs.columns[4+i]:i+1}, inplace=True)
            heat_rate_outputs.iloc[:,i+4] = heat_rate_outputs.iloc[:,i+4].div(heat_rate_outputs.iloc[:,16])
            heat_rate_outputs.drop(heat_rate_outputs.columns[16], axis=1, inplace=True)

        # Get the best heat rate (nearest to full load)
        heat_rate_outputs['Minimum Heat Rate'] = heat_rate_outputs.iloc[:,4:16].min(axis=1)

        hydro_output_path = os.path.join(outputs_directory,'historic_hydro_output.tab')
        write_hydro_output_header = not os.path.isfile(hydro_output_path)
        with open(hydro_output_path, 'ab') as outfile:
            hydro_outputs.to_csv(outfile, sep='\t', header=write_hydro_output_header, encoding='utf-8')
        print "Saved hydro output data to {}.".format(hydro_output_path)

        heat_rate_output_path = os.path.join(outputs_directory,'historic_heat_rates.tab')
        write_heat_rates_header = not os.path.isfile(heat_rate_output_path)
        with open(heat_rate_output_path, 'ab') as outfile:
            heat_rate_outputs.to_csv(outfile, sep='\t', header=write_heat_rates_header, encoding='utf-8')
        print "Saved heat rate data to {}.".format(heat_rate_output_path)


def parse_eia860_dat(directory_list):
    for directory in directory_list:
        year = int(directory[-4:])
        print "============================="
        print "Processing data for year {}.".format(year)

        if year <= 2010:
            rows_to_skip = 0
        else:
            rows_to_skip = 1

        for f in os.listdir(directory):
            # Avoid trying to read a temporal file is any Excel workbook is open
            if 'Plant' in f and '~' not in f:
                path = os.path.join(directory, f)
                plants = pd.read_excel(path, sheetname=0, skiprows=rows_to_skip)
                plants = uniformize_names(plants)
            if 'Generator' in f and '~' not in f:
                path = os.path.join(directory, f)
                existing_generators = uniformize_names(
                    pd.read_excel(path, sheetname=0, skiprows=rows_to_skip))
                existing_generators['Operational Status'] = 'Operable'
                proposed_generators = uniformize_names(
                    pd.read_excel(path, sheetname=1, skiprows=rows_to_skip))
                proposed_generators['Operational Status'] = 'Proposed'
            # Different names for 2008 and previous
            if f.startswith('PRGenY') and '~' not in f:
                path = os.path.join(directory, f)
                proposed_generators = uniformize_names(
                    pd.read_excel(path, sheetname=0, skiprows=rows_to_skip))
                proposed_generators['Operational Status'] = 'Proposed'
            if f.startswith('GenY') and '~' not in f:
                path = os.path.join(directory, f)
                existing_generators = uniformize_names(
                    pd.read_excel(path, sheetname=0, skiprows=rows_to_skip))
                existing_generators['Operational Status'] = 'Operable'
        generators = pd.merge(existing_generators, plants,
            on=['Utility Id','Plant Code', 'Plant Name','State'],
            suffixes=('_units', ''))
        generators = generators.append(proposed_generators)
        print "Read in data for {} existing and {} proposed generation units in "\
            "the US.".format(len(existing_generators), len(proposed_generators))

        # Assign projects to WECC region according to county if no NERC Region
        # is defined. Counties must be located in a WECC state.
        county_list_path = os.path.join(other_data_directory, 'wecc_counties.txt')
        if not os.path.exists(county_list_path):
            print "Database will be queried to obtain list of counties that belong to WECC."
            assign_counties_to_region()
        county_list = list(pd.read_csv(county_list_path, header=None)[0].map(lambda c: str(c).title()))
        generators.loc[
            (generators['County'].map(lambda c: str(c).title()).isin(
                county_list+mispelled_counties)) & 
            (generators['State'].isin(wecc_states)),'Nerc Region'] = 'WECC'

        # Filter to units in the WECC region
        if year != end_year:
            generators = generators[generators['Nerc Region']=='WECC'][gen_relevant_data]
        else:
            generators = generators[generators['Nerc Region']=='WECC'][gen_relevant_data+gen_relevant_data_for_last_year]
        generators.reset_index(drop=True, inplace=True)
        print "Filtered to {} existing and {} proposed generation units in the WECC "\
            "region.".format(len(generators[generators['Operational Status']=='Operable']),
            len(generators[generators['Operational Status']=='Proposed']))

        # Replace chars in numeric columns with null values
        # Most appropriate way would be to replace value with another column
        for col in gen_data_to_be_summed:
            generators[col].replace(' ', 0, inplace=True)
            generators[col].replace('.', 0, inplace=True)

        # Aggregate according to user criteria
        for agg_list in gen_aggregation_lists:
            # Assign unique values to empty cells in columns that will be aggregated upon
            for col in agg_list:
                if generators[col].dtype == np.float64:
                    generators[col].fillna(
                        {i:10000000+i for i in generators.index}, inplace=True)
                else:
                    generators[col].fillna(
                        {i:'None'+str(i) for i in generators.index}, inplace=True)
            gb = generators.groupby(agg_list)
            # Some columns will be summed and all others will get the 'max' value
            if year != end_year:
                generators = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                                for datum in gen_relevant_data})
            else:
                generators = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                                for datum in gen_relevant_data+gen_relevant_data_for_last_year})
            generators.reset_index(drop=True, inplace=True)
            print "Aggregated to {} existing and {} new generation units by aggregating "\
                "through {}.".format(len(generators[generators['Operational Status']=='Operable']),
                len(generators[generators['Operational Status']=='Proposed']), agg_list)
        generators = generators.astype(
            {c: int for c in ['Operating Year', 'Plant Code']})

        if not os.path.exists(outputs_directory):
            os.makedirs(outputs_directory)
        fname = 'generation_projects_{}.tab'.format(year)
        with open(os.path.join(outputs_directory, fname),'w') as f:
            generators.to_csv(f, sep='\t', encoding='utf-8')
        print "Saved data to {} file.".format(fname)


def assign_counties_to_region():
    # assign county if 50% or more of its area falls in the WECC region
    query = "SELECT name \
             FROM ventyx_nerc_reg_region regions CROSS JOIN us_counties counties \
             WHERE regions.gid=13 AND \
             ST_Area(ST_Intersection(counties.the_geom, regions.the_geom))/ST_Area(counties.the_geom)>=0.5"
    wecc_counties = pd.DataFrame(connect_to_db_and_run_query(query=query, database='switch_gis'))
    file_path = os.path.join(other_data_directory, 'wecc_counties.txt')
    with open(file_path, 'w') as f:
        wecc_counties.to_csv(f, header=False, index=False)
    print "Saved list of counties assigned to WECC in {}".format(file_path)

    # Generator costs from schedule 5 are hidden for individual generators,
    # but published in aggregated form. 2015 data is expected to be available
    # in Feb 2017. Data only goes back to 2013; I don't know how to get good
    # estimates of costs of older generators.
    # http://www.eia.gov/electricity/generatorcosts/
    
    # Heat rates: My working plan is to pull plant output and fuel inputs on a
    # monthly basis, calculate average monthly heat rates, inspect the data
    # for a handful of plants and compare the results against generic heat
    # rates for the given generator types. If things look reasonable, use that
    # data series to estimate effective heat rates. Switch would like full
    # load heat rates which may correspond to monthly heat rates where the
    # plant had high cap factors. We could also use average heat rates based
    # on historical cap factors, or we could try to estimate a heat rate curve
    # (aka input-output curve) based on the monthly timeseries. Well, that last
    # approach should probably wait until the second pass.
    # If there are other data sources that I missed that would allow a more
    # direct determination of heat rates, use those instead.
    # Heat rate functionality could live in a separate file.


if __name__ == "__main__":
    main()