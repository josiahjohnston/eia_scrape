# Copyright 2017. All rights reserved. See AUTHORS.txt
# Licensed under the Apache License, Version 2.0 which is in LICENSE.txt
"""
Provides several functions to work with the SWITCH database.

Many are ad-hoc functions for interactive exploration of the data.

To Do:
Push the cleaned and validated data into postgresql.
Assign plants to load zones in postgresql.
Aggregate similar plants within each load zone in postgresql to reduce the
dataset size for the model.

"""

import os
import pandas as pd
from utils import connect_to_db_and_run_query, append_historic_output_to_csv
from IPython import embed

coal_codes = ['ANT','BIT','LIG','SGC','SUB','WC','RC']
outputs_directory = 'processed_data'


def pull_generation_projects_data():
    print "Reading in current generation projects data from database..."
    query = "SELECT * \
            FROM generation_plant JOIN generation_plant_existing_and_planned \
            USING (generation_plant_id) \
            WHERE generation_plant_existing_and_planned_scenario_id = 1 "#AND full_load_heat_rate > 0"
    db_gens = connect_to_db_and_run_query(query=query, database='switch_wecc')
    print "======="
    print "Read in {} projects from the database, with {:.0f} GW of capacity".format(
        len(db_gens), db_gens['capacity'].sum()/1000.0)
    thermal_db_gens = db_gens[db_gens['full_load_heat_rate'] > 0]
    print "Weighted average of heat rate: {:.3f} MMBTU/MWh".format(
        thermal_db_gens['capacity'].dot(thermal_db_gens['full_load_heat_rate'])/thermal_db_gens['capacity'].sum())
    print "======="
    return db_gens

def explore_heat_rates():
    db_gen_projects = pull_generation_projects_data().rename(columns={'name':'Plant Name', 'gen_tech':'Prime Mover'})
    db_gen_projects.loc[:,'Prime Mover'].replace(
        {
        'Coal_Steam_Turbine':'ST',
        'Gas_Steam_Turbine':'ST',
        'Gas_Combustion_Turbine':'GT',
        'Gas_Combustion_Turbine_Cogen':'GT',
        'CCGT':'CC',
        'DistillateFuelOil_Combustion_Turbine':'GT',
        'DistillateFuelOil_Internal_Combustion_Engine':'IC',
        'Geothermal':'ST',
        'Gas_Internal_Combustion_Engine':'IC',
        'Bio_Gas_Internal_Combustion_Engine':'IC',
        'Bio_Gas_Steam_Turbine':'ST'
        },
        inplace=True)
    eia_gen_projects = filter_projects_by_region_id(13)

    df = pd.merge(db_gen_projects, eia_gen_projects,
        on=['Plant Name','Prime Mover'], how='left').loc[:,[
        'Plant Name','gen_tech','energy_source','full_load_heat_rate',
        'Best Heat Rate','Prime Mover','Energy Source','Energy Source 2','Operating Year']]
    df = df[df['full_load_heat_rate']>0]

    print "\nPrinting intersection of DB and EIA generation projects that have a specified heat rate to heat_rate_comparison.tab"
    
    fpath = os.path.join('processed_data','heat_rate_comparison.tab')
    with open(fpath, 'w') as outfile:
        df.to_csv(outfile, sep='\t', header=True, index=False)

    return df

def filter_projects_by_region_id(region_id, year, area=0.5):
    """
    Filters generation project data by NERC Region and assigns Regions to rows
    that do not have one, according to their County and State. Rows will get
    assigned to a Region is more than a certain percentage of the area of the
    county it belongs to intersects with the specified Region.

    Returns a DataFrame with the filtered data.

    """

    state_dict = {
        'Alabama':'AL',
        'Alaska':'AK',
        'Arizona':'AZ',
        'Arkansas':'AR',
        'California':'CA',
        'Colorado':'CO',
        'Connecticut':'CT',
        'Delaware':'DE',
        'Florida':'FL',
        'Georgia':'GA',
        'Hawaii':'HI',
        'Idaho':'ID',
        'Illinois':'IL',
        'Indiana':'IN',
        'Iowa':'IA',
        'Kansas':'KS',
        'Kentucky':'KY',
        'Louisiana':'LA',
        'Maine':'ME',
        'Maryland':'MD',
        'Massachusetts':'MA',
        'Michigan':'MI',
        'Minnesota':'MN',
        'Mississippi':'MS',
        'Missouri':'MO',
        'Montana':'MT',
        'Nebraska':'NE',
        'Nevada':'NV',
        'New Hampshire':'NH',
        'New Jersey':'NJ',
        'New Mexico':'NM',
        'New York':'NY',
        'North Carolina':'NC',
        'North Dakota':'ND',
        'Ohio':'OH',
        'Oklahoma':'OK',
        'Oregon':'OR',
        'Pennsylvania':'PA',
        'Rhode Island':'RI',
        'South Carolina':'SC',
        'South Dakota':'SD',
        'Tennessee':'TN',
        'Texas':'TX',
        'Utah':'UT',
        'Vermont':'VT',
        'Virginia':'VA',
        'Washington':'WA',
        'West Virginia':'WV',
        'Wisconsin':'WI',
        'Wyoming':'WY'
    }

    print "Getting region name from database..."
    query = "SELECT regionabr FROM ventyx_nerc_reg_region WHERE gid={}".format(
        region_id)
    region_name = connect_to_db_and_run_query(query=query,
        database='switch_gis')['regionabr'][0]
    counties_path = os.path.join('other_data', '{}_counties.tab'.format(region_name))
    
    if not os.path.exists(counties_path):
        # assign county if (area)% or more of its area falls in the region
        query = "SELECT name, state\
                 FROM ventyx_nerc_reg_region regions CROSS JOIN us_counties cts\
                 JOIN (SELECT DISTINCT state, state_fips FROM us_states) sts \
                 ON (sts.state_fips=cts.statefp) \
                 WHERE regions.gid={} AND\
                 ST_Area(ST_Intersection(cts.the_geom, regions.the_geom))/\
                 ST_Area(cts.the_geom)>={}".format(region_id, area)
        print "\nGetting counties and states for the region from database..."
        region_counties = pd.DataFrame(connect_to_db_and_run_query(query=query,
            database='switch_gis')).rename(columns={'name':'County','state':'State'})
        region_counties.replace(state_dict, inplace=True)
        region_counties.to_csv(counties_path, sep='\t', index=False)
    else:
        print "Reading counties from .tab file..."
        region_counties = pd.read_csv(counties_path, sep='\t', index_col=None)

    generators = pd.read_csv(
        os.path.join('processed_data','generation_projects_{}.tab'.format(year)), sep='\t')
    generators.loc[:,'County'] = generators['County'].map(lambda c: str(c).title())

    print "\nRead in data for {} generators, of which:".format(len(generators))
    print "--{} are existing".format(len(generators[generators['Operational Status']=='Operable']))
    print "--{} are proposed".format(len(generators[generators['Operational Status']=='Proposed']))

    generators_with_assigned_region = generators.loc[generators['Nerc Region'] == region_name]
    generators = generators[generators['Nerc Region'].isnull()]
    generators_without_assigned_region = pd.merge(generators, region_counties, how='inner', on=['County','State'])
    generators = pd.concat([
        generators_with_assigned_region,
        generators_without_assigned_region],
        axis=0)
    generators.replace(
            to_replace={'Energy Source':coal_codes, 'Energy Source 2':coal_codes,
            'Energy Source 3':coal_codes}, value='COAL', inplace=True)
    generators_columns = list(generators.columns)

    existing_gens = generators[generators['Operational Status']=='Operable']
    proposed_gens = generators[generators['Operational Status']=='Proposed']

    print "======="
    print "Filtered to {} projects in the {} region, of which:".format(
        len(generators), region_name)
    print "--{} are existing with {:.0f} GW of capacity".format(
        len(existing_gens), existing_gens['Nameplate Capacity (MW)'].sum()/1000.0)
    print "--{} are proposed with {:.0f} GW of capacity".format(
        len(proposed_gens), proposed_gens['Nameplate Capacity (MW)'].sum()/1000.0)
    print "======="

    return generators


def assign_heat_rates_to_projects(generators, year):
    existing_gens = generators[generators['Operational Status']=='Operable']
    print "-------------------------------------"
    print "There are {} existing thermal projects that sum up to {:.1f} GW.".format(
        len(existing_gens[existing_gens['Prime Mover'].isin(['CC','GT','IC','ST'])]),
        existing_gens[existing_gens['Prime Mover'].isin(['CC','GT','IC','ST'])][
            'Nameplate Capacity (MW)'].sum()/1000)
    heat_rate_data = pd.read_csv(
        os.path.join('processed_data','historic_heat_rates_WIDE.tab'), sep='\t').rename(
        columns={'Plant Code':'EIA Plant Code'})
    heat_rate_data = heat_rate_data[heat_rate_data['Year']==year]
    thermal_gens = pd.merge(
        existing_gens, heat_rate_data[['EIA Plant Code','Prime Mover','Energy Source','Best Heat Rate']],
        how='left', suffixes=('',''),
        on=['EIA Plant Code','Prime Mover','Energy Source']).drop_duplicates()


    thermal_gens = thermal_gens[thermal_gens['Prime Mover'].isin(['CC','GT','IC','ST'])]
    fuels = {
        'LFG':'Bio_Gas',
        'OBG':'Bio_Gas',
        'AB':'Bio_Solid',
        'BLQ':'Bio_Liquid',
        'NG':'Gas',
        'OG':'Gas',
        'PG':'Gas',
        'DFO':'DistillateFuelOil',
        'JF':'ResidualFuelOil',
        'COAL':'Coal',
        'GEO':'Geothermal',
        'NUC':'Nuclear',
        'PC':'Coal',
        'SUN':'Solar',
        'WDL':'Bio_Liquid',
        'WDS':'Bio_Solid',
        'MSW':'Bio_Solid',
        'PUR':'Purchased_Steam',
        'WH':'Waste_Heat',
        'OTH':'Other'
    }
    thermal_gens = thermal_gens.replace({'Energy Source':fuels})
    existing_gens = existing_gens.replace({'Energy Source':fuels})

    # Replace null and unrealistic heat rates by average values per technology,
    # fuel, and vintage. Also, set HR of top and bottom .5% to max and min
    null_heat_rates = thermal_gens['Best Heat Rate'].isnull()
    unrealistic_heat_rates = (((thermal_gens['Energy Source'] == 'Coal') &
            (thermal_gens['Best Heat Rate'] < 8.607)) |
        ((thermal_gens['Energy Source'] != 'Coal') &
            (thermal_gens['Best Heat Rate'] < 6.711)))
    print "{} generators don't have heat rate data specified ({:.1f} GW of capacity)".format(
        len(thermal_gens[null_heat_rates]), thermal_gens[null_heat_rates]['Nameplate Capacity (MW)'].sum()/1000.0)
    print "{} generators have better heat rate than the best historical records ({} GW of capacity)\n".format(
        len(thermal_gens[unrealistic_heat_rates]), thermal_gens[unrealistic_heat_rates]['Nameplate Capacity (MW)'].sum()/1000.0)
    thermal_gens_w_hr = thermal_gens[~null_heat_rates & ~unrealistic_heat_rates]
    thermal_gens_wo_hr = thermal_gens[null_heat_rates | unrealistic_heat_rates]

    # Print fuels and technologies with missing HR to console

    # for fuel in thermal_gens_wo_hr['Energy Source'].unique():
    #     print "{} of these use {} as their fuel".format(
    #         len(thermal_gens_wo_hr[thermal_gens_wo_hr['Energy Source']==fuel]),fuel)
    #     print "Technologies:"
    #     for prime_mover in thermal_gens_wo_hr[thermal_gens_wo_hr['Energy Source']==fuel]['Prime Mover'].unique():
    #         print "\t{} use {}".format(
    #             len(thermal_gens_wo_hr[(thermal_gens_wo_hr['Energy Source']==fuel) &
    #                 (thermal_gens_wo_hr['Prime Mover']==prime_mover)]),prime_mover)

    print "Assigning max/min heat rates per technology and fuel to top .5% / bottom .5%, respectively:"
    n_outliers = int(len(thermal_gens_w_hr)*0.008)
    thermal_gens_w_hr = thermal_gens_w_hr.sort_values('Best Heat Rate')
    min_hr = thermal_gens_w_hr.loc[thermal_gens_w_hr.index[n_outliers],'Best Heat Rate']
    max_hr = thermal_gens_w_hr.loc[thermal_gens_w_hr.index[-1-n_outliers],'Best Heat Rate']
    print "(Total capacity of these plants is {:.1f} GW)".format(
        thermal_gens_w_hr[thermal_gens_w_hr['Best Heat Rate'] < min_hr]['Nameplate Capacity (MW)'].sum()/1000.0 +
        thermal_gens_w_hr[thermal_gens_w_hr['Best Heat Rate'] > max_hr]['Nameplate Capacity (MW)'].sum()/1000.0)
    print "Minimum heat rate is {:.3f}".format(min_hr)
    print "Maximum heat rate is {:.3f}".format(max_hr)
    for i in range(n_outliers):
        thermal_gens_w_hr.loc[thermal_gens_w_hr.index[i],'Best Heat Rate'] = min_hr
        thermal_gens_w_hr.loc[thermal_gens_w_hr.index[-1-i],'Best Heat Rate'] = max_hr


    def calculate_avg_heat_rate(thermal_gens_df, prime_mover, energy_source, vintage, window=2):
        similar_generators = thermal_gens_df[
            (thermal_gens_df['Prime Mover']==prime_mover) &
            (thermal_gens_df['Energy Source']==energy_source) &
            (thermal_gens_df['Operating Year']>=vintage-window) &
            (thermal_gens_df['Operating Year']<=vintage+window)]
        while len(similar_generators) < 4:
            window += 2
            similar_generators = thermal_gens_df[
                (thermal_gens_df['Prime Mover']==prime_mover) &
                (thermal_gens_df['Energy Source']==energy_source) &
                (thermal_gens_df['Operating Year']>=vintage-window) &
                (thermal_gens_df['Operating Year']<=vintage+window)]
            # Gens span from 1925 to 2015, so a window of 90 years is the maximum
            if window >= 90:
                break
        if len(similar_generators) > 0:
            return similar_generators['Best Heat Rate'].mean()
        else:
            # If no other similar projects exist, return average of technology
            return thermal_gens_df[thermal_gens_df['Prime Mover']==prime_mover]['Best Heat Rate'].mean()


    print "Assigning average heat rates per technology, fuel, and vintage to projects w/o heat rate..."
    for idx in thermal_gens_wo_hr.index:
        pm = thermal_gens_wo_hr.loc[idx,'Prime Mover']
        es = thermal_gens_wo_hr.loc[idx,'Energy Source']
        v = thermal_gens_wo_hr.loc[idx,'Operating Year']
        #print "{}\t{}\t{}\t{}".format(pm,es,v,calculate_avg_heat_rate(thermal_gens_w_hr, pm, es, v))
        thermal_gens_wo_hr.loc[idx,'Best Heat Rate'] = calculate_avg_heat_rate(
            thermal_gens_w_hr, pm, es, v)

    thermal_gens = pd.concat([thermal_gens_w_hr, thermal_gens_wo_hr], axis=0)
    existing_gens = pd.merge(existing_gens, thermal_gens, on=list(existing_gens.columns), how='left')


    proposed_gens = generators[generators['Operational Status']=='Proposed']
    thermal_proposed_gens = proposed_gens[proposed_gens['Prime Mover'].isin(['CC','GT','IC','ST'])]
    other_proposed_gens = proposed_gens[~proposed_gens['Prime Mover'].isin(['CC','GT','IC','ST'])]
    print "-------------------------------------"
    print "There are {} proposed thermal projects that sum up to {:.2f} GW.".format(
        len(thermal_proposed_gens), thermal_proposed_gens['Nameplate Capacity (MW)'].sum()/1000)
    print "Assigning average heat rate of technology and fuel of most recent years..."
    for idx in thermal_proposed_gens.index:
        pm = thermal_proposed_gens.loc[idx,'Prime Mover']
        es = thermal_proposed_gens.loc[idx,'Energy Source']
        #print "{}\t{}\t{}\t{}".format(pm,es,v,calculate_avg_heat_rate(thermal_gens_w_hr, pm, es, v))
        thermal_proposed_gens.loc[idx,'Best Heat Rate'] = calculate_avg_heat_rate(
            thermal_gens_w_hr, pm, es, year)

    proposed_gens = pd.concat([thermal_proposed_gens,other_proposed_gens], axis=0)


    #fig,axes = plt.subplots(2,2,figsize=7,7)
    #nominal_heat_rates = generators[~generators['Best Heat Rate'].isnull()]['Best Heat Rate']
    #for i,pm in enumerate(['CC','GT','IC','ST']):
    #    axes[0,0].hist(nominal_heat_rates, bins=tuple(i for i in range(0,50,1)))
    #    ax.set_title('')
    #    ax.set_xlabel(xlabel)
    #    ax.set_ylabel(ylabel)
    #    ax.set_xlim((-xlim,xlim))
    #    ax.plot((0,0),(0,ax.get_ylim()[1]), linewidth=0.4, color='black')

    return pd.concat([existing_gens, proposed_gens], axis=0)


def finish_project_processing(year):
    generators = filter_projects_by_region_id(13, year)
    generators = assign_heat_rates_to_projects(generators, year)
    existing_gens = generators[generators['Operational Status']=='Operable']
    proposed_gens = generators[generators['Operational Status']=='Proposed']

    fname = 'existing_generation_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        existing_gens.to_csv(f, sep='\t', encoding='utf-8', index=False)

    uprates = pd.DataFrame()
    new_gens = pd.DataFrame()
    for idx in proposed_gens.index:
        pc = proposed_gens.loc[idx,'EIA Plant Code']
        pm = proposed_gens.loc[idx,'Prime Mover']
        es = proposed_gens.loc[idx,'Energy Source']
        existing_units_for_proposed_gen = existing_gens[
        (existing_gens['EIA Plant Code'] == pc) &
        (existing_gens['Prime Mover'] == pm) &
        (existing_gens['Energy Source'] == es)]
        if len(existing_units_for_proposed_gen) == 0:
            new_gens = pd.concat([new_gens, pd.DataFrame(proposed_gens.loc[idx,:]).T], axis=0)
        elif len(existing_units_for_proposed_gen) == 1:
            uprates = pd.concat([uprates, pd.DataFrame(proposed_gens.loc[idx,:]).T], axis=0)
        else:
            print "There is more than one option for uprating plant id {}, prime mover {} and energy source {}".format(pc, pm, es)

    fname = 'new_generation_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        new_gens.to_csv(f, sep='\t', encoding='utf-8', index=False)

    fname = 'uprates_to_generation_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        uprates.to_csv(f, sep='\t', encoding='utf-8', index=False)