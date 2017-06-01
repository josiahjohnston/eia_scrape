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

import pandas as pd

misspelled_counties = [
    'Claveras'
    ]

def filter_dataframe_by_region_id(df, region_id, area=0.5):
    """
    Filters a dataframe by NERC Region and assigns Regions to rows that do not
    have one, according to their County and State. Rows will get assigned to a
    Region is more than a certain percentage of the area of the county it
    belongs to intersects with the specified Region.

    To Do:
    Use lat & long for existing plants. Only use county and state names for
    proposed plants that don't have coordinates.

    """

    query = "SELECT regionabr FROM ventyx_nerc_reg_region WHERE gid={}".format(
        region_id)
    region_name = connect_to_db_and_run_query(query=query,
        database='switch_gis')[0][0]
    # assign county if (area)% or more of its area falls in the region
    query = "SELECT name\
             FROM ventyx_nerc_reg_region regions CROSS JOIN us_counties counties\
             WHERE regions.gid={} AND\
             ST_Area(ST_Intersection(counties.the_geom, regions.the_geom))/\
             ST_Area(counties.the_geom)>={}".format(region_id, area)
    counties = pd.DataFrame(connect_to_db_and_run_query(query=query,
        database='switch_gis'))
    
    generators.loc[generators['Nerc Region'].isnull()]

    df = df.loc[(df['Nerc Region'] == region_name) |
        ((df['County'].map(lambda c: str(c).title()).isin(counties)) & 
        (df['State'].isin(region_states)))] 
    
    df.reset_index(drop=True, inplace=True)
    print ("Filtered to {} existing and {} proposed generation units in the {} "
        "region.".format(region_name,
            len(df[df['Operational Status']=='Operable']),
            len(df[df['Operational Status']=='Proposed'])))
    return df

