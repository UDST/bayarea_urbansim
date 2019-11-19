from __future__ import print_function

import orca
import numpy as np
import pandas as pd
from urbansim_defaults import utils
from baus import datasources
from baus import variables
from baus import summaries

from operator import itemgetter
import itertools


# after slr has inundated some parcels and removed buildings permanently,
# earthquake model removes further buildings temporarily

@orca.step()
def eq_code_buildings(buildings, year, scenario, hazards):

    if scenario not in hazards["eq_scenarios"]["enable_in"]:
        return

    if year == 2035:
        # tags buildings that exist in 2035 with a fragility coefficient
        # keeping in-model adds run time, but is important given developer
        # model stochastisitcy, that will change the building stock in 2035
        # this also allows us to change the building codes when retrofitting
        # policies are applied, thus changing fragility coefficients
        buildings = buildings.to_frame()
        code = []
        fragilities = []

        for i in buildings.index:
            if (buildings['building_type'][i] == 'HS' and
               buildings['year_built'][i] <= 2015):
                a = 'SF'
                if buildings['stories'][i] == 1:
                    b = '01'
                    if buildings['year_built'][i] <= 1940:
                        c = 'G1'
                    elif (buildings['year_built'][i] >= 1941 and
                          buildings['year_built'][i] <= 1960):
                        c = 'G2'
                    elif (buildings['year_built'][i] >= 1961 and
                          buildings['year_built'][i] <= 1995):
                        c = 'G3'
                    elif (buildings['year_built'][i] >= 1996 and
                          buildings['year_built'][i] <= 2015):
                        c = 'G4'
                elif buildings['stories'][i] >= 2:
                    b = '2P'
                    if buildings['year_built'][i] <= 1920:
                        c = 'G1'
                    elif (buildings['year_built'][i] >= 1921 and
                          buildings['year_built'][i] <= 1940):
                        c = 'G2'
                    elif (buildings['year_built'][i] >= 1941 and
                          buildings['year_built'][i] <= 1960):
                        c = 'G3'
                    elif (buildings['year_built'][i] >= 1961 and
                          buildings['year_built'][i] <= 1995):
                        c = 'G4'
                    elif (buildings['year_built'][i] >= 1996 and
                          buildings['year_built'][i] <= 2015):
                        c = 'G5'
            elif ((buildings['building_type'][i] == 'HM' or
                  buildings['building_type'][i] == 'MR') and
                  buildings['year_built'][i] <= 2015):
                if (buildings['residential_units'][i] == 2 or
                   buildings['residential_units'][i] == 3 or
                   buildings['residential_units'][i] == 4):
                    a = 'DU'  # 2, 3, & 4 units
                    # are considered duplex/triplex/quadplex
                    if buildings['stories'][i] == 1:
                        b = '01'
                        if buildings['year_built'][i] <= 1940:
                            c = 'G1'
                        elif (buildings['year_built'][i] >= 1941 and
                              buildings['year_built'][i] <= 1960):
                            c = 'G2'
                        elif (buildings['year_built'][i] >= 1961 and
                              buildings['year_built'][i] <= 1995):
                            c = 'G3'
                        elif (buildings['year_built'][i] >= 1996 and
                              buildings['year_built'][i] <= 2015):
                            c = 'G4'
                    if buildings['stories'][i] >= 2:
                        b = '2P'
                        if buildings['year_built'][i] <= 1920:
                            c = 'G1'
                        elif (buildings['year_built'][i] >= 1921 and
                              buildings['year_built'][i] <= 1940):
                            c = 'G2'
                        elif (buildings['year_built'][i] >= 1941 and
                              buildings['year_built'][i] <= 1960):
                            c = 'G3'
                        elif (buildings['year_built'][i] >= 1961 and
                              buildings['year_built'][i] <= 1977):
                            c = 'G4'
                        elif (buildings['year_built'][i] >= 1978 and
                              buildings['year_built'][i] <= 1991):
                            c = 'G5'
                        elif (buildings['year_built'][i] >= 1992 and
                              buildings['year_built'][i] <= 2015):
                            c = 'G6'
                else:  # this assumes one-unit HM/MR buildings
                    # are also 5+ units (multifamily split by parcels)
                    a = 'MF'
                    if buildings['stories'][i] == 1:
                        b = '01'
                        if buildings['year_built'][i] <= 1920:
                            c = 'G1'
                        elif (buildings['year_built'][i] >= 1921 and
                              buildings['year_built'][i] <= 1940):
                            c = 'G2'
                        elif (buildings['year_built'][i] >= 1941 and
                              buildings['year_built'][i] <= 1960):
                            c = 'G3'
                        elif (buildings['year_built'][i] >= 1961 and
                              buildings['year_built'][i] <= 1995):
                            c = 'G4'
                        elif (buildings['year_built'][i] >= 1996 and
                              buildings['year_built'][i] <= 2015):
                            c = 'G5'
                    elif (buildings['stories'][i] >= 2 and
                          buildings['stories'][i] <= 5):
                        b = '25'
                        if buildings['year_built'][i] <= 1920:
                            c = 'G1'
                        elif (buildings['year_built'][i] >= 1921 and
                              buildings['year_built'][i] <= 1940):
                            c = 'G2'
                        elif (buildings['year_built'][i] >= 1941 and
                              buildings['year_built'][i] <= 1960):
                            c = 'G3'
                        elif (buildings['year_built'][i] >= 1961 and
                              buildings['year_built'][i] <= 1977):
                            c = 'G4'
                        elif (buildings['year_built'][i] >= 1978 and
                              buildings['year_built'][i] <= 1991):
                            c = 'G5'
                        elif (buildings['year_built'][i] >= 1992 and
                              buildings['year_built'][i] <= 2015):
                            c = 'G6'
                    elif buildings['stories'][i] >= 6:
                        b = '5P'
                        if buildings['year_built'][i] <= 1950:
                            c = 'G1'
                        elif (buildings['year_built'][i] >= 1951 and
                              buildings['year_built'][i] <= 1971):
                            c = 'G2'
                        elif (buildings['year_built'][i] >= 1972 and
                              buildings['year_built'][i] <= 1995):
                            c = 'G3'
                        elif (buildings['year_built'][i] >= 1996 and
                              buildings['year_built'][i] <= 2006):
                            c = 'G4'
                        elif (buildings['year_built'][i] >= 2007 and
                              buildings['year_built'][i] <= 2015):
                            c = 'G5'
            elif buildings['year_built'][i] <= 2015:
                a = 'OT'
                b = 'NN'
                if buildings['year_built'][i] <= 1933:
                    c = 'G1'
                elif (buildings['year_built'][i] >= 1934 and
                      buildings['year_built'][i] <= 1950):
                    c = 'G2'
                elif (buildings['year_built'][i] >= 1951 and
                      buildings['year_built'][i] <= 1972):
                    c = 'G3'
                elif (buildings['year_built'][i] >= 1973 and
                      buildings['year_built'][i] <= 1996):
                    c = 'G4'
                elif (buildings['year_built'][i] >= 1997 and
                      buildings['year_built'][i] <= 2006):
                    c = 'G5'
                elif (buildings['year_built'][i] >= 2007 and
                      buildings['year_built'][i] <= 2015):
                    c = 'G6'
            # new buildings built by the developer model
            elif buildings['year_built'][i] > 2015:
                a = 'NN'
                b = 'NN'
                c = 'NN'
                # alternative if retrofitted: d = 'R'
            d = 'N'
            code_i = a+b+c+d
            code.append(code_i)

            # assign a fragility coefficient based on building code
            if (code_i == 'SF01G4N' or code_i == 'SF2PG5N' or
               code_i == 'DU2PG6N' or code_i == 'MF5PG5N' or
               code_i == 'DU01G4N' or code_i == 'MF25G6N' or
               code_i == 'MF01G5N' or code_i == 'OTNNG6N'):
                fragility = 1
            elif (code_i == 'SF01G3N' or code_i == 'DU01G3N' or
                  code_i == 'DU2PG5N' or code_i == 'MF25G5N' or
                  code_i == 'MF01G4N' or code_i == 'OTNNG5N' or
                  code_i == 'MF5PG4N'):
                fragility = 1.2
            elif (code_i == 'SF2PG4N' or code_i == 'MF5PG3N' or
                  code_i == 'OTNNG4N'):
                fragility = 1.3
            elif (code_i == 'MF5PG1N' or code_i == 'OTNNG2N'):
                fragility = 1.4
            elif (code_i == 'MF01G3N' or code_i == 'MF5PG2N' or
                  code_i == 'SF01G2N' or code_i == 'DU01G2N' or
                  code_i == 'OTNNG3N'):
                fragility = 1.5
            elif (code_i == 'DU2PG3N' or code_i == 'DU2PG4N'):
                fragility = 1.75
            elif (code_i == 'SF2PG3N' or code_i == 'DU01G1N' or
                  code_i == 'DU2PG2N' or code_i == 'MF01G2N' or
                  code_i == 'OTNNG1N'):
                fragility = 2
            elif (code_i == 'SF2PG2N'):
                fragility = 2.25
            elif (code_i == 'DU2PG1N' or code_i == 'SF01G1N' or
                  code_i == 'SF2PG1N' or code_i == 'MF01G1N' or
                  code_i == 'MF25G1N'):
                fragility = 2.5
            elif (code_i == 'MF25G2N' or code_i == 'MF25G3N' or
                  code_i == 'MF25G4N'):
                fragility = 3
            elif (code_i == 'NNNNNNN'):
                fragility = 0
            fragilities.append(fragility)

        orca.add_injectable("code", code)
        orca.add_injectable("fragilities", fragilities)

        # add codes and fragilities as orca columns
        code = pd.Series(code, buildings.index)
        orca.add_column('buildings', 'earthquake_code', code)
        fragility = pd.Series(fragilities, buildings.index)
        orca.add_column('buildings', 'fragility_coef', fragility)

        # generate random number, multiply by fragilities
        buildings = orca.get_table('buildings')
        rand_eq = np.random.random(len(buildings))
        destroy_eq = pd.Series(rand_eq*fragility)
        orca.add_column('buildings', 'eq_destroy', destroy_eq)

        # generate random number for fire
        rand_fire = pd.Series(np.random.random(len(buildings)))
        orca.add_column('buildings', 'fire_destroy', rand_fire)


@orca.step()
def earthquake_demolish(parcels, parcels_tract, tracts_earthquake, buildings,
                        households, jobs, residential_units, year, scenario,
                        hazards):

    if scenario not in hazards["eq_scenarios"]["enable_in"]:
        return

    if year == 2035:
        # assign each parcel to a census tract using the lookup table
        # created with scripts/parcel_tract_assignment.py
        census_tract = pd.Series(parcels_tract['census_tract'],
                                 parcels_tract.index)
        print("Number of parcels with census tracts is: %d" %
              len(census_tract))
        orca.add_column('parcels', 'tract', census_tract)

        # group parcels by their census tract
        parcels_tract['parcel_id'] = parcels_tract.index
        parcels_tract = parcels_tract.to_frame(columns=['parcel_id',
                                                        'census_tract'])
        parcels_tract = parcels_tract[['census_tract', 'parcel_id']]
        tract_parcels_grp = []
        tracts = []
        parcels_tract = sorted(parcels_tract.values, key=itemgetter(0))
        for tract, parcels in itertools.groupby(parcels_tract,
                                                key=itemgetter(0)):
            tract_parcels_grp.append(list(parcels))
            tracts.append(tract)
        print("Number of census tract groups is: %d" % len(tract_parcels_grp))

        # for the parcels in each tract, destroy X% of parcels in that tract
        tracts_earthquake = tracts_earthquake.to_frame()
        tracts_earthquake = tracts_earthquake.sort_values(by=['tract_ba'])
        tracts_earthquake = tracts_earthquake.reset_index(drop=True)

        buildings = buildings.to_frame()
        eq_buildings = []
        existing_buildings = []
        new_buildings = []
        fire_buildings = []
        retrofit_bldgs_tot = pd.DataFrame()

        for i in range(len(tracts)):
            grp = [x[1] for x in tract_parcels_grp[i]]
            buildings_i = buildings[buildings['parcel_id'].isin(grp)]

            # existing buildings
            # select the buildings with highest fragility co-efficient
            # (and random no.) based on census tract pct to be destroyed
            existing_pct = tracts_earthquake['prop_eq'][i]
            build_frag = buildings_i['eq_destroy'].sort_values(ascending=False)
            top_build_frag = build_frag[: int(round(
                len(build_frag) * existing_pct))]
            # in "strategies" scenarios, exclude some existing buildings
            # from destruction due to retrofit
            if scenario in hazards["eq_scenarios"]["mitigation"]:
                retrofit_codes = ['DU01G1N', 'DU01G2N', 'MF01G1N', 'MF01G2N',
                                  'MF25G1N', 'MF25G2N', 'MF25G3N', 'MF25G4N',
                                  'SF01G1N', 'SF2PG1N']
                top_build_frag_bldgs = buildings[buildings.index.isin
                                                 (top_build_frag.index)]
                retrofit_bldgs = top_build_frag_bldgs[top_build_frag_bldgs.
                                                      earthquake_code.isin
                                                      (retrofit_codes)]
                retro_no = int(round(float(len(retrofit_bldgs))/2))
                retrofit_set = np.random.choice(retrofit_bldgs.index,
                                                retro_no, replace=False)
                # update top_build_frag to remove retrofit buildings
                top_build_frag = top_build_frag[~top_build_frag.index.isin
                                                (retrofit_set)]
                # add table of retrofit buildings that weren't destroyed
                retrofit_bldgs_set = buildings[buildings.index.isin
                                               (retrofit_set)]
                retrofit_bldgs_tot = retrofit_bldgs_tot. \
                    append(retrofit_bldgs_set)
                orca.add_table("retrofit_bldgs_tot", retrofit_bldgs_tot)
            # add to a list of buildings to destroy
            buildings_top = top_build_frag.index
            existing_buildings.extend(buildings_top)
            eq_buildings.extend(buildings_top)

            # new buildings
            # translate MMI to a probability
            # in-model is also nice if probabilities associated with
            # new buildings change
            buildings_new = buildings_i[buildings_i['year_built'] > 2015]
            if len(buildings_new) > 0:
                mmi = int(round(tracts_earthquake['shaking'][i]))
                if mmi < 6:
                    new_pct = 0
                elif mmi == 7:
                    new_pct = .002
                elif mmi == 8:
                    new_pct = .01
                elif mmi == 9:
                    new_pct = .05
                # randomly select buildings to be destroyed based on
                # percentages
                new_no = int(round(len(buildings_new)*new_pct))
                buildings_new_rand = np.random.choice(buildings_new.index,
                                                      new_no, replace=False)
                # add to a list of buildings to destroy
                if len(buildings_new_rand) > 0:
                    new_buildings.extend(buildings_new_rand)
                    eq_buildings.extend(buildings_new_rand)

            # fire buildings
            # select buildings to be destroyed by fire by looking only at
            # remaining buildings
            fire_pct = tracts_earthquake['prop_fire'][i]
            buildings_i_remain = buildings_i[~buildings_i.index.isin
                                             (buildings_top)]
            if len(buildings_new) > 0:
                buildings_i_remain = buildings_i_remain[~buildings_i_remain.
                                                        index.isin
                                                        (buildings_new_rand)]
            # select buildings to be destroyed based on random number
            # and census tract pct
            fire_buildings_rand = buildings_i_remain['fire_destroy']. \
                sort_values(ascending=False)
            top_fire_buildings = fire_buildings_rand[: int(round(
                len(fire_buildings_rand) * fire_pct))]
            # add to a list of buildings to destroy
            buildings_fire = top_fire_buildings.index
            fire_buildings.extend(buildings_fire)
            eq_buildings.extend(buildings_fire)

        print("Total number of buildings being destroyed is: %d" %
              len(eq_buildings))

        orca.add_injectable("eq_buildings", eq_buildings)
        orca.add_injectable("existing_buildings", existing_buildings)
        orca.add_injectable("new_buildings", new_buildings)
        orca.add_injectable("fire_buildings", fire_buildings)

        # remove buildings, unplace agents
        buildings = orca.get_table('buildings')
        eq_demolish = buildings.local[buildings.index.isin
                                      (eq_buildings)]
        orca.add_table("eq_demolish", eq_demolish)
        print("Demolishing %d buildings" % len(eq_demolish))

        households = households.to_frame()
        hh_unplaced = households[households["building_id"] == -1]
        jobs = jobs.to_frame()
        jobs_unplaced = jobs[jobs["building_id"] == -1]

        l1 = len(buildings)
        # currently destroying more buildings than it is being
        # passed- why?
        buildings = utils._remove_developed_buildings(
            buildings.to_frame(buildings.local_columns),
            eq_demolish,
            unplace_agents=["households", "jobs"])

        households = orca.get_table("households")
        households = households.to_frame()
        hh_unplaced_eq = households[households["building_id"] == -1]
        hh_unplaced_eq = hh_unplaced_eq[~hh_unplaced_eq.index.isin
                                        (hh_unplaced.index)]
        orca.add_injectable("hh_unplaced_eq", hh_unplaced_eq)
        jobs = orca.get_table("jobs")
        jobs = jobs.to_frame()
        jobs_unplaced_eq = jobs[jobs["building_id"] == -1]
        jobs_unplaced_eq = jobs_unplaced_eq[~jobs_unplaced_eq.index.isin
                                            (jobs_unplaced.index)]
        orca.add_injectable("jobs_unplaced_eq", jobs_unplaced_eq)

        orca.add_table("buildings", buildings)
        buildings = orca.get_table("buildings")
        print("Demolished %d buildings" % (l1 - len(buildings)))
