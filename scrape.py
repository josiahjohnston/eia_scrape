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
Calculate hydro outputs previous to 2004 with nameplate capacities of that year,
but first check that uprating is not significant for hydro plants.
Write a report with assumptions taken and observations noticed during the writing
of these scripts and data analysis.
Move region filtering code to separate script.

To Do in separate scripts:
Push the cleaned and validated data into postgresql.
Assign plants to load zones in postgresql.
Aggregate similar plants within each load zone in postgresql to reduce the
dataset size for the model.

"""

import csv, os, re
import numpy as np
import pandas as pd
from calendar import monthrange

# Update the reference to the utils module after this becomes a package
from utils import download_file, download_metadata_fields, unzip

unzip_directory = 'downloads'
pickle_directory = 'pickle_data'
other_data_directory = 'other_data'
outputs_directory = 'processed_data'
download_log_path = os.path.join(unzip_directory, 'download_log.csv')
REUSE_PRIOR_DOWNLOADS = True
CLEAR_PRIOR_OUTPUTS = True
REWRITE_PICKLES = False
start_year, end_year = 2015,2015
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
    for directory in (unzip_directory, other_data_directory, outputs_directory, pickle_directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    if CLEAR_PRIOR_OUTPUTS:
        for f in os.listdir(outputs_directory):
            os.remove(os.path.join(outputs_directory,f))

    zip_file_list = scrape_eia860()
    unzip(zip_file_list)
    eia860_directory_list = [os.path.splitext(f)[0] for f in zip_file_list]
    for eia860_annual_filing in eia860_directory_list:
        parse_eia860_data(eia860_annual_filing)

    zip_file_list = scrape_eia923()
    unzip(zip_file_list)
    eia923_directory_list = [os.path.splitext(f)[0] for f in zip_file_list]
    for eia923_annual_filing in eia923_directory_list:
        parse_eia923_data(eia923_annual_filing)


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


def parse_eia923_data(directory):
    year = int(directory[-4:])
    print "============================="
    print "Processing data for year {}.".format(year)
    
    # First, try saving data as pickle if it hasn't been done before
    # Reading pickle files is orders of magnitude faster than reading Excel
    # files directly. This saves tons of time when re-running the script.
    pickle_path = os.path.join(pickle_directory,'eia923_{}.pickle'.format(year))
    if not os.path.exists(pickle_path) or REWRITE_PICKLES:
        print "Pickle file has to be written for this EIA923 form. Creating..."
        # Name of the relevant spreadsheet is not consistent throughout years
        # Read largest file in the directory instead of looking by name
        largest_file = max([os.path.join(directory, f)
            for f in os.listdir(directory)], key=os.path.getsize)
        # Different number of blank rows depending on year
        if year >= 2011:
            rows_to_skip = 5
        else:
            rows_to_skip = 7
        generation = uniformize_names(pd.read_excel(largest_file,
            sheetname='Page 1 Generation and Fuel Data', skiprows=rows_to_skip))
        generation.to_pickle(pickle_path)
    else:
        print "Pickle file exists for this EIA923. Reading..."
        generation = pd.read_pickle(pickle_path)
    
    generation.loc[:,'Year'] = year
    # Get column order for easier month matching later on
    column_order = list(generation.columns)
    # Remove "State-Fuel Level Increment" fictional plants
    generation = generation.loc[generation['Plant Code']!=99999]
    print ("Read in EIA923 fuel and generation data for {} generation units "
           "and plants in the US.").format(len(generation))

    # Replace characters with proper nan values
    numeric_columns = [col for col in generation.columns if 
        re.compile('(?i)elec[_\s]mmbtu').match(col) or re.compile('(?i)netgen').match(col)]
    for col in numeric_columns:
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

    def append_historic_output_to_csv(fname, df):
        output_path = os.path.join(outputs_directory, fname)
        write_header = not os.path.isfile(output_path)
        with open(output_path, 'ab') as outfile:
            df.to_csv(outfile, sep='\t', header=write_header, encoding='utf-8', index=False)

    #############################
    # Save hydro profiles

    def df_to_long_format(df, col_name, month, index_cols):
        return pd.melt(df, index_cols, '{} Month {}'.format(col_name, month)
            ).drop('variable',axis=1).rename(columns={'value':col_name})

    ###############
    # WIDE format
    hydro_outputs=pd.concat([
        hydro_generation[['Year','Plant Code','Plant Name','Prime Mover']],
        hydro_generation.filter(regex=r'(?i)netgen')
        ], axis=1)
    hydro_outputs=pd.merge(hydro_outputs, hydro_gen_projects[['Plant Code','Prime Mover','Nameplate Capacity (MW)']],
        on=['Plant Code','Prime Mover'], suffixes=('',''))
    for month in range(1,13):
        hydro_outputs.rename(
            columns={hydro_outputs.columns[3+month]:'Net Electricity Generation (MWh) Month {}'.format(month)},
            inplace=True)
        hydro_outputs.loc[:,'Capacity Factor Month {}'.format(month)] = \
            hydro_outputs.loc[:,'Net Electricity Generation (MWh) Month {}'.format(month)].div(
            monthrange(int(year),month)[1]*24*hydro_outputs['Nameplate Capacity (MW)'])

    append_historic_output_to_csv('historic_hydro_capacity_factors_WIDE.tab', hydro_outputs)
    print "Saved hydro capacity factor data in wide format for {}.".format(year)

    ###############
    # NARROW format
    index_columns = [
            'Year',
            'Plant Code',
            'Plant Name',
            'Prime Mover'
        ]
    hydro_outputs_narrow = pd.DataFrame(columns=['Month'])
    for month in range(1,13):
        hydro_outputs_narrow = pd.concat([
            hydro_outputs_narrow,
            pd.merge(
                df_to_long_format(hydro_outputs, 'Capacity Factor', month, index_columns),
                df_to_long_format(hydro_outputs, 'Net Electricity Generation (MWh)', month, index_columns),
                on=index_columns)
            ], axis=0)
        hydro_outputs_narrow.loc[:,'Month'].fillna(month, inplace=True)

    # Get friendlier output
    hydro_outputs_narrow = hydro_outputs_narrow[['Month', 'Year',
            'Plant Code', 'Plant Name', 'Prime Mover', 'Capacity Factor',
            'Net Electricity Generation (MWh)']]
    hydro_outputs_narrow = hydro_outputs_narrow.astype(
            {c: int for c in ['Month', 'Year', 'Plant Code']})

    append_historic_output_to_csv('historic_hydro_capacity_factors_NARROW.tab', hydro_outputs_narrow)
    print "Saved hydro capacity factor data in narrow format for {}.".format(year)

    #############################
    # Save heat rate profiles

    ###############
    # WIDE format
    heat_rate_outputs=pd.concat([
        fuel_based_generation[
            ['Plant Code','Plant Name','Prime Mover','Energy Source','Year']],
            fuel_based_generation.filter(regex=r'(?i)elec[_\s]mmbtu'),
            fuel_based_generation.filter(regex=r'(?i)netgen')
        ], axis=1)
    heat_rate_outputs=pd.merge(heat_rate_outputs,
        fuel_based_gen_projects[['Plant Code','Prime Mover','Nameplate Capacity (MW)']],
        on=['Plant Code','Prime Mover'], suffixes=('',''))

    # Get total fuel consumption per plant and prime mover
    total_fuel_consumption = pd.concat([
            fuel_based_generation[
                ['Plant Code','Prime Mover']],
                fuel_based_generation.filter(regex=r'(?i)elec[_\s]mmbtu')
            ], axis=1)
    total_fuel_consumption.rename(columns={
        total_fuel_consumption.columns[1+m]:'Fraction of Total Fuel Consumption Month {}'.format(m)
        for m in range(1,13)}, inplace=True)
    total_fuel_consumption_columns = list(total_fuel_consumption.columns)
    gb = total_fuel_consumption.groupby(['Plant Code','Prime Mover'])
    total_fuel_consumption = gb.agg({col:('max' if col in ['Plant Code','Prime Mover'] else sum)
                                    for col in total_fuel_consumption_columns}).reset_index(drop=True)
    total_fuel_consumption = total_fuel_consumption[total_fuel_consumption_columns]
    heat_rate_outputs=pd.merge(heat_rate_outputs, total_fuel_consumption,
            on=['Plant Code','Prime Mover'], suffixes=('',''))

    # To Do: Use regex filtering for this in case number of columns changes
    for month in range(1,13):
        heat_rate_outputs.rename(
            columns={heat_rate_outputs.columns[4+month]:'Heat Rate Month {}'.format(month)},
            inplace=True)
        heat_rate_outputs.rename(
            columns={heat_rate_outputs.columns[16+month]:'Net Electricity Generation (MWh) Month {}'.format(month)},
            inplace=True)
        # Calculate fraction of total fuel use
        heat_rate_outputs.loc[:,'Fraction of Total Fuel Consumption Month {}'.format(month)] = \
            heat_rate_outputs.iloc[:,month+4].div(
            heat_rate_outputs.loc[:,'Fraction of Total Fuel Consumption Month {}'.format(month)])
        # Heat rates
        heat_rate_outputs.loc[:,'Heat Rate Month {}'.format(month)] = \
            heat_rate_outputs.loc[:,'Heat Rate Month {}'.format(month)].div(
                heat_rate_outputs.loc[:,'Net Electricity Generation (MWh) Month {}'.format(month)])
        # Capacity factors
        heat_rate_outputs['Capacity Factor Month {}'.format(month)] = \
            heat_rate_outputs.loc[:,'Net Electricity Generation (MWh) Month {}'.format(month)].div(
                monthrange(int(year),month)[1]*24*heat_rate_outputs['Nameplate Capacity (MW)'])

    # Get the best heat rate in a separate column
    heat_rate_outputs['Minimum Heat Rate'] = heat_rate_outputs[heat_rate_outputs>0].iloc[:,5:17].min(axis=1)

    append_historic_output_to_csv('historic_heat_rates_WIDE.tab', heat_rate_outputs)
    print "Saved heat rate data in wide format for {}.".format(year)

    ###############
    # NARROW format
    index_columns = [
            'Year',
            'Plant Code',
            'Plant Name',
            'Prime Mover',
            'Energy Source'
        ]
    heat_rate_outputs_narrow = pd.DataFrame(columns=['Month'])
    for month in range(1,13):
        # To Do: Collapse the mergers into a more compact function
        heat_rate_outputs_narrow = pd.concat([
            heat_rate_outputs_narrow,
            pd.merge(
                pd.merge(
                    pd.merge(
                        df_to_long_format(heat_rate_outputs, 'Heat Rate', month, index_columns),
                        df_to_long_format(heat_rate_outputs, 'Capacity Factor', month, index_columns),
                    on=index_columns),
                    df_to_long_format(heat_rate_outputs, 'Net Electricity Generation (MWh)', month, index_columns),
                    on=index_columns),
                df_to_long_format(heat_rate_outputs, 'Fraction of Total Fuel Consumption', month, index_columns),
                on=index_columns)
            ], axis=0)
        heat_rate_outputs_narrow.loc[:,'Month'].fillna(month, inplace=True)

    # Get friendlier output
    heat_rate_outputs_narrow = heat_rate_outputs_narrow[['Month', 'Year',
            'Plant Code', 'Plant Name', 'Prime Mover', 'Energy Source',
            'Heat Rate', 'Capacity Factor', 'Fraction of Total Fuel Consumption',
            'Net Electricity Generation (MWh)']]
    heat_rate_outputs_narrow = heat_rate_outputs_narrow.astype(
            {c: int for c in ['Month', 'Year', 'Plant Code']})

    append_historic_output_to_csv('historic_heat_rates_NARROW.tab', heat_rate_outputs_narrow)
    print "Saved heat rate data in narrow format for {}.".format(year)


def parse_eia860_data(directory):
    year = int(directory[-4:])
    print "============================="
    print "Processing data for year {}.".format(year)

    # First, try saving data as pickle if it hasn't been done before
    # Reading pickle files is orders of magnitude faster than reading Excel
    # files directly. This saves tons of time when re-running the script.
    pickle_path_plants = os.path.join(pickle_directory,'eia860_{}_plants.pickle'.format(year))
    pickle_path_existing_generators = os.path.join(pickle_directory,'eia860_{}_existing.pickle'.format(year))
    pickle_path_proposed_generators = os.path.join(pickle_directory,'eia860_{}_proposed.pickle'.format(year))
    
    if not os.path.exists(pickle_path_plants) \
        or not os.path.exists(pickle_path_existing_generators) \
            or not os.path.exists(pickle_path_proposed_generators) \
                or REWRITE_PICKLES:
        print "Pickle files have to be written for this EIA860 form. Creating..."
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
            # In any case, all files have to be read differently, so I'm not
            # sure that the code would become any cleaner by using Glob.

            # From 2009 onwards, look for files with "Plant" and "Generator"
            # in their name.
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
        
        plants.to_pickle(pickle_path_plants)
        existing_generators.to_pickle(pickle_path_existing_generators)
        proposed_generators.to_pickle(pickle_path_proposed_generators)
    else:
        print "Pickle files exist for this EIA860. Reading..."
        plants = pd.read_pickle(pickle_path_plants)
        existing_generators = pd.read_pickle(pickle_path_existing_generators)
        proposed_generators = pd.read_pickle(pickle_path_proposed_generators)

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
        generators[col].replace(' ', float('nan'), inplace=True)
        generators[col].replace('.', float('nan'), inplace=True)

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