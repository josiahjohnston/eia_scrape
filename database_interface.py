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
misspelled_counties = [
    'Claveras'
    ]


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
        'Minimum Heat Rate','Prime Mover','Energy Source','Energy Source 2','Operating Year']]
    df = df[df['full_load_heat_rate']>0]

    print "\nPrinting intersection of DB and EIA generation projects that have a specified heat rate to heat_rate_comparison.tab"
    
    fpath = os.path.join('processed_data','heat_rate_comparison.tab')
    with open(fpath, 'w') as outfile:
        df.to_csv(outfile, sep='\t', header=True, index=False)

    return df

def filter_projects_by_region_id(region_id, area=0.5):
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
        os.path.join('processed_data','generation_projects_2015.tab'), sep='\t')
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

    print "\nThere are {} existing projects that are not solar, wind, hydro or batteries.".format(
        len(existing_gens[~existing_gens['Energy Source'].isin(
            ['SUN','WND','WAT','MWH'])]))
    heat_rate_data = pd.read_csv(
        os.path.join('processed_data','historic_heat_rates_WIDE.tab'), sep='\t').rename(
        columns={'Plant Code':'EIA Plant Code'})
    heat_rate_data = heat_rate_data[heat_rate_data['Year']==2015]
    generators = pd.merge(
        existing_gens, heat_rate_data[['EIA Plant Code','Prime Mover','Energy Source','Minimum Heat Rate']],
        how='left', suffixes=('',''),
        on=['EIA Plant Code','Prime Mover','Energy Source']).drop_duplicates()

    thermal_gens = generators[~generators['Minimum Heat Rate'].isnull()]
    print "There is heat rate data for {} of these generators".format(len(thermal_gens))
    append_historic_output_to_csv(
        os.path.join('processed_data','gens_wo_heat_rate.tab'),
        generators[
        (generators['Minimum Heat Rate'].isnull()) &
        (~generators['Energy Source'].isin(['SUN','WND','WAT','MWH']))])
    print "(Generators without heat rate data were printed to gens_wo_heat_rate.tab)"
    print "{} of these records are below 30 MMBTU/MWh".format(len(thermal_gens[thermal_gens['Minimum Heat Rate'] <= 30]))
    print "There is heat rate data for {} generators that are not solar, wind, hydro or batteries.".format(
        len(thermal_gens[~thermal_gens['Energy Source'].isin(
            ['SUN','WND','WAT','MWH'])]))
    thermal_gens = thermal_gens[thermal_gens['Minimum Heat Rate'] <= 30]
    print "Weighted heat rate average of these generators: {:.3f} MMBTU/MWh".format(
        thermal_gens['Minimum Heat Rate'].dot(thermal_gens['Nameplate Capacity (MW)'])/thermal_gens['Nameplate Capacity (MW)'].sum())

    return generators

