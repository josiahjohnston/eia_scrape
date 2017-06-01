# Copyright 2017. All rights reserved. See AUTHORS.txt
# Licensed under the Apache License, Version 2.0 which is in LICENSE.txt
"""
Provides several functions to work with the SWITCH database.

To Do:
Push the cleaned and validated data into postgresql.
Assign plants to load zones in postgresql.
Aggregate similar plants within each load zone in postgresql to reduce the
dataset size for the model.

"""

import os
import pandas as pd
from utils import connect_to_db_and_run_query
from IPython import embed

misspelled_counties = [
    'Claveras'
    ]

#def push_generation_projects_data():

def pull_generation_projects_data():
    query = "SELECT * \
            FROM generation_plant JOIN generation_plant_existing_and_planned \
            USING (generation_plant_id) \
            WHERE generation_plant_existing_and_planned_scenario_id = 1 AND \
            full_load_heat_rate > 0"
    return connect_to_db_and_run_query(query=query, database='switch_wecc')

def explore_heat_rates():
    heat_rate_outputs = pd.read_csv(
        os.path.join('processed_data','historic_heat_rates_WIDE.tab'), sep='\t')
    heat_rate_outputs = heat_rate_outputs[heat_rate_outputs['Year']==2015]
    heat_rate_outputs.rename(columns={'Plant Name':'name'}, inplace=True)
    db_gen_projects = pull_generation_projects_data()
    name_intersection = heat_rate_outputs[heat_rate_outputs['name'].isin(
        db_gen_projects['name'])]['name']
    return pd.merge(
        db_gen_projects[db_gen_projects['name'].isin(
            name_intersection)][['name','gen_tech','energy_source','full_load_heat_rate']],
        heat_rate_outputs[heat_rate_outputs['name'].isin(
            name_intersection)][['name','Minimum Heat Rate','Prime Mover','Energy Source','Energy Source 2']],
        how='right', on='name')

def filter_projects_by_region_id(region_id, area=0.5):
    """
    Filters a dataframe by NERC Region and assigns Regions to rows that do not
    have one, according to their County and State. Rows will get assigned to a
    Region is more than a certain percentage of the area of the county it
    belongs to intersects with the specified Region.

    To Do:
    Use lat & long for existing plants. Only use county and state names for
    proposed plants that don't have coordinates.

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

    generators = pd.read_csv(
        os.path.join('processed_data','generation_projects_2015.tab'), sep='\t')
    generators.loc[:,'County'] = generators['County'].map(lambda c: str(c).title())

    print "\nRead in data for {} generators, of which:".format(len(generators))
    print "--{} are existing".format(len(generators[generators['Operational Status']=='Operable']))
    print "--{} are proposed".format(len(generators[generators['Operational Status']=='Proposed']))

    existing_generators_in_region = generators.loc[generators['Nerc Region'] == region_name]
    generators = generators[generators['Nerc Region'].isnull()]
    proposed_generators_in_region = pd.merge(generators, region_counties, how='inner', on=['County','State'])
    generators = pd.concat([
        existing_generators_in_region,
        proposed_generators_in_region],
        axis=0)
    
    print "\nFiltered to projects in the {} region, of which:".format(region_name)
    print "--{} are existing".format(len(generators[generators['Operational Status']=='Operable']))
    print "--{} are proposed".format(len(generators[generators['Operational Status']=='Proposed']))

    return generators

