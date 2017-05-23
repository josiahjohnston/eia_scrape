# Copyright 2017. All rights reserved. See AUTHORS.txt
# Licensed under the Apache License, Version 2.0 which is in LICENSE.txt
"""
Scrape data on existing and planned generators in the United States from the 
Energy Information Agency's EIA860 and EIA923 forms (and their older versions).

Enables sequential aggregation of generator data by multiple criteria and
filtering projects of specific NERC Regions.

Extracts monthly capacity factors for each hydroelectric generation plant.

Extracts monthly heat rate factors for each thermal generation plant.

All data is scrapped and parsed from 2004 onwards.

To Do:
Better error checks.
More QA/QC on output.
Print hydro capacity factors and heat rates in "short format", so that .tab files
can be easily uploaded to the database. Do this IN ADDITION to current outputs,
so that manual inspection can be easily done in the "long format" files.
Calculate hydro outputs previous to 2004 with nameplate capacities of that year,
but first check that uprating is not significant for hydro plants.
Write a report with assumptions taken and observations noticed during the writing
of these scripts and data analysis.


"""

import csv, os, re
import numpy as np
import pandas as pd

# Update the reference to the utils module after this becomes a package
from utils import download_file, download_metadata_fields, unzip

unzip_directory = 'downloads'
other_data_directory = 'other_dat'
outputs_directory = 'processed_data'
download_log_path = os.path.join(unzip_directory, 'download_log.csv')
REUSE_PRIOR_DOWNLOADS = True
CLEAR_PRIOR_OUTPUTS = True
start_year, end_year = 2011,2015
fuel_prime_movers = ['ST','GT','IC','CA','CT','CS','CC']
region_states = ['WA','OR','CA','AZ','NV','NM','UT','ID','MT','WY','CO','TX']
accepted_status_codes = ['OP','SB','CO','SC','OA','OZ','TS','L','T','U','V']
gen_relevant_data = ['Plant Code', 'Plant Name', 'Status', 'Nameplate Capacity (MW)',
                    'Prime Mover', 'Energy Source', 'County', 'State', 'Nerc Region',
                    'Operating Year', 'Planned Retirement Year',
                    'Generator Id', 'Unit Code', 'Operational Status']
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
misspelled_counties = [
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
        'Proposed Nameplate':'Nameplate Capacity (MW)',
        'Proposed Status':'Status',
        'Eia Plant Code':'Plant Code'
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
    parse_eia860_data([os.path.splitext(f)[0] for f in zip_file_list])

    zip_file_list = scrape_eia923()
    unzip(zip_file_list)
    parse_eia923_data([os.path.splitext(f)[0] for f in zip_file_list])


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
    write_log_header = not os.path.isfile(download_log_path)
    with open(download_log_path, 'ab') as logfile:
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
    write_log_header = not os.path.isfile(download_log_path)
    with open(download_log_path, 'ab') as logfile:
        logwriter = csv.writer(logfile, delimiter='\t',
                               quotechar="'", quoting=csv.QUOTE_MINIMAL)
        if write_log_header:
            logwriter.writerow(download_metadata_fields)
        logwriter.writerows(log_dat)
    
    return [os.path.join(unzip_directory, f) for f in file_list]


def parse_eia923_data(directory_list):
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
        # Get column order for easier month matching later on
        column_order = list(generation.columns)
        # Remove "State-Fuel Level Increment" fictional plants
        generation = generation.loc[generation['Plant Code']!=99999]
        print ("Read in EIA923 fuel and generation data for {} generation units "
               "and plants in the US.").format(len(generation))

        # Replace characters with zeros when no value is provided
        # Josiah: I'm wary of replacing blank values with 0's because blanks can
        # mean no data is available, while 0 has a specific numeric meaning. I'm
        # more comfortable using NaN's, and eventually filtering out those months.
        # Is this ok?
        numeric_columns = [col for col in generation.columns if 
            re.compile('(?i)elec[_\s]mmbtu').match(col) or re.compile('(?i)netgen').match(col)]
        for col in numeric_columns:
    #         generation[col].replace(' ', 0, inplace=True)
    #         generation[col].replace('.', 0, inplace=True)
            generation[col].replace(' ', float('nan'), inplace=True)
            generation[col].replace('.', float('nan'), inplace=True)

        # Aggregated generation of plants. First assign CC as prime mover for combined cycles.
        generation.loc[generation['Prime Mover'].isin(['CA','CT','CS']),'Prime Mover']='CC'
        gb = generation.groupby(['Plant Code','Prime Mover','Energy Source'])
        generation = gb.agg({datum:('max' if datum not in numeric_columns else sum)
                                        for datum in generation.columns})
        hydro_generation = generation[generation['Energy Source']=='WAT']
        fuel_based_generation = generation[generation['Prime Mover'].isin(fuel_prime_movers)]
        print ("Aggregated generation data to {} generation plants through Plant "
               "Code, Prime Mover and Energy Source.").format(len(generation))
        print "\tHydro projects:{}".format(len(hydro_generation))
        print "\tFuel based projects:{}".format(len(fuel_based_generation))
        print "\tOther projects:{}".format(
            len(generation) - len(fuel_based_generation) - len(hydro_generation))

        # Reload a summary of generation projects for nameplate capacity.
        generation_projects = uniformize_names(pd.read_csv(
            os.path.join(outputs_directory,'generation_projects_{}.tab').format(year),
            sep='\t'))
        print ("Read in processed EIA860 plant data for {} generation units in "
               "the US").format(len(generation_projects))
        gb = generation_projects.groupby(['Plant Code','Prime Mover','Operational Status'])
        generation_projects = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                                        for datum in generation_projects.columns})
        hydro_gen_projects = generation_projects[
            (generation_projects['Operational Status']=='Operable') &
            (generation_projects['Energy Source']=='WAT')]
        fuel_based_gen_projects = generation_projects[
            (generation_projects['Operational Status']=='Operable') &
            (generation_projects['Prime Mover'].isin(fuel_prime_movers))]
        print ("Aggregated plant data into {} generation plants through Plant Code "
               "and Prime Mover.").format(len(generation_projects))
        print "\tHydro projects:{}".format(len(hydro_gen_projects))
        print "\tFuel based projects:{}".format(len(fuel_based_gen_projects))
        print "\tOther projects:{}".format(
            len(generation_projects) - len(fuel_based_gen_projects) - len(hydro_gen_projects))

        # Cross-check data and print console messages with gaps.
        def check_overlap_proj_and_production(projects, production, gen_type, log_path):
            """
            Look for generation projects from EIA860 that don't have production
            data available from form EIA923 and vice versa. Print console messages
            with summaries.
            """
            # Projects with plant data, but no production data
            filter = projects['Plant Code'].isin(production['Plant Code'])
            projects_missing_production = projects[~filter]
            missing_MW = projects_missing_production['Nameplate Capacity (MW)'].sum()
            total_MW = projects['Nameplate Capacity (MW)'].sum()
            print ("{} of {} {} generation projects in the EIA860 plant form "
                   "are not in the EIA923 form, {:.4f}% total {} capacity "
                   "({:.0f} of {:.0f} MW)."
                  ).format(
                    len(projects_missing_production),
                    len(projects),
                    gen_type,
                    100 * (missing_MW / total_MW),
                    gen_type,
                    missing_MW, total_MW,
                  )
            summary = projects_missing_production.groupby(['Plant Code', 'Plant Name']).sum()
            summary['Net Generation (Megawatthours)'] = float('NaN')
            summary.to_csv(log_path, 
                columns=['Nameplate Capacity (MW)', 'Net Generation (Megawatthours)'])
        
            # Projects with generation data, but no plant data
            filter = production['Plant Code'].isin(projects['Plant Code'])
            production_missing_project = production[~filter]
            missing_MWh = production_missing_project['Net Generation (Megawatthours)'].sum()
            total_MWh = production['Net Generation (Megawatthours)'].sum()
            print ("{} of {} {} generation projects in the EIA923 generation form "
                   "are not in the EIA860 plant form: {:.4f}% "
                   "total annual {} production ({:.0f} of {:.0f} MWh)."
                  ).format(
                    len(production_missing_project), len(production), 
                    gen_type,
                    100 * (missing_MWh / total_MWh),
                    gen_type,
                    missing_MWh, total_MWh, 
                  )
            summary = production_missing_project.groupby(['Plant Code', 'Plant Name']).sum()
            summary['Nameplate Capacity (MW)'] = float('NaN')
            summary.to_csv(log_path, mode='a', header=False, 
                columns=['Nameplate Capacity (MW)', 'Net Generation (Megawatthours)'])
            print ("Summarized {} plants with missing data to {}."
                  ).format(gen_type, log_path)


        # Check for projects that have plant data but no generation data, and vice versa
        log_path = os.path.join(outputs_directory, 'incomplete_data_hydro_{}.csv'.format(year))
        check_overlap_proj_and_production(hydro_gen_projects, hydro_generation,
                                          'hydro', log_path)
        log_path = os.path.join(outputs_directory, 'incomplete_data_thermal_{}.csv'.format(year))
        check_overlap_proj_and_production(fuel_based_gen_projects, fuel_based_generation, 
                                          'thermal', log_path)

        # Recover original column order
        hydro_generation = hydro_generation[column_order]
        fuel_based_generation = fuel_based_generation[column_order]

        # Save hydro profiles
        hydro_outputs=pd.concat([
            hydro_generation[['Plant Code','Plant Name','Prime Mover']],
            hydro_generation.filter(regex=r'(?i)netgen')
            ], axis=1)
        hydro_outputs.loc[:,'Year']=year
        hydro_outputs=pd.merge(hydro_outputs, hydro_gen_projects[['Plant Code','Prime Mover','Nameplate Capacity (MW)']],
            on=['Plant Code','Prime Mover'], suffixes=('',''))
        for i,m in enumerate(months):
            hydro_outputs.rename(columns={hydro_outputs.columns[3+i]:i+1}, inplace=True)
            hydro_outputs.loc[:,i+1] = hydro_outputs.loc[:,i+1].div(months[m]*24*hydro_outputs['Nameplate Capacity (MW)'])
        
        # Save heat rate profiles
        heat_rate_outputs=pd.concat([
            fuel_based_generation[
                ['Plant Code','Plant Name','Prime Mover','Energy Source']],
                fuel_based_generation.filter(regex=r'(?i)elec[_\s]mmbtu'),
                fuel_based_generation.filter(regex=r'(?i)netgen')
            ], axis=1)
        heat_rate_outputs.loc[:,'Year']=year
        heat_rate_outputs=pd.merge(heat_rate_outputs,
            fuel_based_gen_projects[['Plant Code','Prime Mover','Nameplate Capacity (MW)']],
            on=['Plant Code','Prime Mover'], suffixes=('',''))

        # To Do: Use regex filtering for this in case number of columns changes
        for i,m in enumerate(months):
            heat_rate_outputs.rename(columns={heat_rate_outputs.columns[4+i]:i+1}, inplace=True)
            heat_rate_outputs.iloc[:,i+4] = heat_rate_outputs.iloc[:,i+4].div(heat_rate_outputs.iloc[:,16])
            heat_rate_outputs.drop(heat_rate_outputs.columns[16], axis=1, inplace=True)

        # Get the best heat rate in a separate column (ignore negative values,
        # which I think correspond to cogenerators)
        heat_rate_outputs['Minimum Heat Rate'] = heat_rate_outputs[heat_rate_outputs>=0].iloc[:,4:16].min(axis=1)

        hydro_output_path = os.path.join(outputs_directory,'historic_hydro_output.tab')
        write_hydro_output_header = not os.path.isfile(hydro_output_path)
        with open(hydro_output_path, 'ab') as outfile:
            hydro_outputs.to_csv(outfile, sep='\t', header=write_hydro_output_header, encoding='utf-8')
        print "Saved hydro output data to {}.".format(hydro_output_path)

        heat_rate_output_path = os.path.join(outputs_directory,'historic_heat_rates.tab')
        write_heat_rates_header = not os.path.isfile(heat_rate_output_path)
        with open(heat_rate_output_path, 'ab') as outfile:
            heat_rate_outputs.to_csv(outfile, sep='\t', header=write_heat_rates_header, encoding='utf-8')
        print "Saved heat rate data to {}.\n".format(heat_rate_output_path)


def parse_eia860_data(directory_list):
    for directory in directory_list:
        year = int(directory[-4:])
        print "============================="
        print "Processing data for year {}.".format(year)

        # Different number of blank rows depending on year
        if year <= 2010:
            rows_to_skip = 0
        else:
            rows_to_skip = 1

        for f in os.listdir(directory):
            path = os.path.join(directory, f)
            # Use a simple for loop, since for years previous to 2008, there are
            # multiple ocurrences of "GenY" in files. Haven't found a clever way
            # to do a pattern search with Glob that excludes unwanted files.
            # In any case, all files are read differently with Pandas, so I'm not
            # sure that the code would become any cleaner by using Glob.

            # From 2009 onwards, look for files with "Plant" and "Generator" in its
            # name.
            # Avoid trying to read a temporal file if any Excel workbook is open
            if 'Plant' in f and '~' not in f:
                plants = uniformize_names(
                    pd.read_excel(path, sheetname=0, skiprows=rows_to_skip))
            if 'Generator' in f and '~' not in f:
                existing_generators = uniformize_names(
                    pd.read_excel(path, sheetname=0, skiprows=rows_to_skip))
                existing_generators['Operational Status'] = 'Operable'
                proposed_generators = uniformize_names(
                    pd.read_excel(path, sheetname=1, skiprows=rows_to_skip))
                proposed_generators['Operational Status'] = 'Proposed'
            # Different names from 2008 backwards
            if f.startswith('PRGenY'):
                proposed_generators = uniformize_names(
                    pd.read_excel(path, sheetname=0, skiprows=rows_to_skip))
                proposed_generators['Operational Status'] = 'Proposed'
            if f.startswith('GenY'):
                existing_generators = uniformize_names(
                    pd.read_excel(path, sheetname=0, skiprows=rows_to_skip))
                existing_generators['Operational Status'] = 'Operable'
        generators = pd.merge(existing_generators, plants,
            on=['Utility Id','Plant Code', 'Plant Name','State'],
            suffixes=('_units', ''))
        generators = generators.append(proposed_generators)
        print "Read in data for {} existing and {} proposed generation units in "\
            "the US.".format(len(existing_generators), len(proposed_generators))

        # Filter projects according to status
        generators = generators.loc[generators['Status'].isin(accepted_status_codes)]
        print "Filtered to {} existing and {} proposed generation units by removing inactive "\
            "and planned projects not yet started.".format(
                len(generators[generators['Operational Status']=='Operable']),
                len(generators[generators['Operational Status']=='Proposed']))

        # Replace chars in numeric columns with null values
        # Most appropriate way would be to replace value with another column
        for col in gen_data_to_be_summed:
            generators[col].replace(' ', 0, inplace=True)
            generators[col].replace('.', 0, inplace=True)

        # Manually set Prime Mover of combined cycle plants before aggregation
        generators.loc[generators['Prime Mover'].isin(['CA','CT','CS']),'Prime Mover'] = 'CC'

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
            # Columns are reordered after aggregation for easier inspection
            if year != end_year:
                generators = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                                for datum in gen_relevant_data}).loc[:,gen_relevant_data]
            else:
                generators = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                                for datum in gen_relevant_data+gen_relevant_data_for_last_year}).loc[
                                :,gen_relevant_data+gen_relevant_data_for_last_year]
            generators.reset_index(drop=True, inplace=True)
            print "Aggregated to {} existing and {} new generation units by aggregating "\
                "through {}.".format(len(generators[generators['Operational Status']=='Operable']),
                len(generators[generators['Operational Status']=='Proposed']), agg_list)

        # Write some columns as ints for easier inspection
        generators = generators.astype(
            {c: int for c in ['Operating Year', 'Plant Code']})
        # Drop columns that are no longer needed
        generators = generators.drop(['Unit Code','Generator Id'], axis=1)
        # Add EIA prefix to be explicit about code origin
        generators = generators.rename(columns={'Plant Code':'EIA Plant Code'})

        if not os.path.exists(outputs_directory):
            os.makedirs(outputs_directory)
        fname = 'generation_projects_{}.tab'.format(year)
        with open(os.path.join(outputs_directory, fname),'w') as f:
            generators.to_csv(f, sep='\t', encoding='utf-8', index=False)
        print "Saved data to {} file.\n".format(fname)


def assign_counties_to_region(region_id, area=0.5):
    """
    Reads geographic data for US counties and assigns them to a NERC Region,
    producing a simple text file with the list of counties which have more than
    a certain % of area intersection with the specified Region.

    """

    query = "SELECT regionabr FROM ventyx_nerc_reg_region WHERE gid={}".format(region_id)
    region_name = connect_to_db_and_run_query(query=query, database='switch_gis')[0][0]
    # assign county if (area)% or more of its area falls in the region
    query = "SELECT name\
             FROM ventyx_nerc_reg_region regions CROSS JOIN us_counties counties\
             WHERE regions.gid={} AND\
             ST_Area(ST_Intersection(counties.the_geom, regions.the_geom))/\
             ST_Area(counties.the_geom)>={}".format(region_id, area)
    wecc_counties = pd.DataFrame(connect_to_db_and_run_query(query=query,
        database='switch_gis'))
    file_path = os.path.join(other_data_directory, '{}_counties.txt'.format(
        region_name))
    with open(file_path, 'w') as f:
        wecc_counties.to_csv(f, header=False, index=False)
    print "Saved list of counties assigned to the {} Region in {}".format(
        region_name, file_path)


def filter_dataframe_by_region_id(df, region_id):
    """
    Filters a dataframe by NERC Region and assigns Regions to rows that
    do not have one, according to County and State.

    Should be migrated to a separate file to work directly with the DB.

    """

    # WECC region id: 13
    query = "SELECT regionabr FROM ventyx_nerc_reg_region WHERE gid={}".format(region_id)
    region_name = connect_to_db_and_run_query(query=query, database='switch_gis')[0][0]
    
    # Assign projects to region according to county if no NERC Region is defined.
    county_list_path = os.path.join(other_data_directory, '{}_counties.txt'.format(region_name))
    if not os.path.exists(county_list_path):
        print "Database will be queried to obtain the list of counties that belong to the {} region.".format(region_name)
        assign_counties_to_region(region_id, 0.5)
    county_list = list(pd.read_csv(county_list_path, header=None)[0].map(lambda c: str(c).title()))
    
    # If region is not defined, use County and State to filter
    # WARNING: This filter is not perfect
    generators.loc[generators['Nerc Region'].isnull()]

    df = df.loc[(df['Nerc Region'] == region_name) |
        ((df['County'].map(lambda c: str(c).title()).isin(
            county_list+misspelled_counties)) & 
        (df['State'].isin(region_states)))] 
    
    df.reset_index(drop=True, inplace=True)
    print "Filtered to {} existing and {} proposed generation units in the {} "\
        "region.".format(region_name,
            len(df[df['Operational Status']=='Operable']),
            len(df[df['Operational Status']=='Proposed']))
    return df


# Generator costs from schedule 5 are hidden for individual generators,
# but published in aggregated form. 2015 data is expected to be available
# in Feb 2017. Data only goes back to 2013; I don't know how to get good
# estimates of costs of older generators.
# http://www.eia.gov/electricity/generatorcosts/


if __name__ == "__main__":
    main()