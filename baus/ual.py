from __future__ import print_function

import os
import math
import yaml
import numpy as np
import pandas as pd
import orca
import orca_test as ot
from orca_test import OrcaSpec, TableSpec, ColumnSpec, InjectableSpec
from urbansim.developer.developer import Developer as dev
from urbansim.models.relocation import RelocationModel
from urbansim.utils import misc
from urbansim_defaults import utils


###############################################################################
#
# (1) UAL ORCA STEPS FOR DATA MODEL INITIALIZATION
#
###############################################################################


def _create_empty_units(buildings):
    """
    Create a table of empty units corresponding to an input table of buildings.
    This function is used (a) in initialization and (b) after the developer
    model steps run.

    Parameters
    ----------
    buildings : DataFrameWrapper or DataFrame
        Must contain an index to be used as the building identifier, and a
        count of 'residential_units' which will determine the number of
        units to create

    Returns
    -------
    df : DataFrame
        Table of units, to be processed within an orca step
    """
    # The '.astype(int)' deals with a bug (?) where the developer model creates
    # floating-point unit counts

    s = buildings.residential_units.fillna(0) >=\
        buildings.deed_restricted_units.fillna(0)

    assert np.all(buildings.residential_units.fillna(0) >=
                  buildings.deed_restricted_units.fillna(0))

    df = pd.DataFrame({
        'unit_residential_price': 0.0,
        'unit_residential_rent': 0.0,
        'num_units': 1,
        'building_id': np.repeat(
            buildings.index.values,
            buildings.residential_units.values.astype(int)
        ),
        # counter of the units in a building
        'unit_num': np.concatenate([
            np.arange(num_units)
            for num_units in buildings.residential_units.values.astype(int)
        ]),
        # also identify deed restricted units
        'deed_restricted': np.concatenate([
            np.concatenate([
                np.ones(restricted_units),
                np.zeros(num_units - restricted_units)
            ])
            # iterate over number of units and deed restricted units too
            for (num_units, restricted_units) in list(zip(
                buildings.residential_units.values.astype(int),
                buildings.deed_restricted_units.values.astype(int)
            ))
        ])
    }).sort_values(by=['building_id', 'unit_num']).reset_index(drop=True)
    df.index.name = 'unit_id'
    return df


def match_households_to_units(households, residential_units):
    """
    This initialization step adds a 'unit_id' to the households table and
    populates it based on existing assignments of households to buildings.
    This also allows us to add a 'vacant_units' count to the residential_units
    table.  FSF note: this won't work if there are more households in a
    building than there are units in that building - make sure not to have
    overfull buildings.

    Data expectations
    -----------------
    - 'households' table has NO column 'unit_id'
    - 'households' table has column 'building_id' (int, '-1'-filled,
      corresponds to index of 'buildings' table)
    - 'residential_units' table has an index that serves as its id,
      and following columns:
        - 'building_id' (int, non-missing, corresponds to index of
          'buildings' table)
        - 'unit_num' (int, non-missing, unique within building)

    Results
    -------
    - adds following column to 'households' table:
        - 'unit_id' (int, '-1'-filled, corresponds to index of
          'residential_units' table)
    """
    units = residential_units
    hh = households

    # This code block is from Fletcher
    unit_lookup = units.reset_index().set_index(['building_id', 'unit_num'])
    hh = hh.sort_values(by=['building_id'], ascending=True)

    building_counts = hh.building_id.value_counts().sort_index()
    hh['unit_num'] = np.concatenate(
        [np.arange(i) for i in building_counts.values])

    unplaced = hh[hh.building_id == -1].index
    placed = hh[hh.building_id != -1].index

    indexes = [tuple(t) for t in
               hh.loc[placed, ['building_id', 'unit_num']].values]

    hh.loc[placed, 'unit_id'] = unit_lookup.loc[indexes].unit_id.values
    hh.loc[unplaced, 'unit_id'] = -1

    return hh


def assign_tenure_to_units(residential_units, households):
    """
    This initialization step assigns tenure to residential units, based on the
    'tenure' attribute of the households occupying them. (Tenure for
    unoccupied units is assigned andomly.)

    Data expections
    ---------------
    - 'residential_units' table has NO column 'tenure'
    - 'households' table has following columns:
        - 'tenure' (str, either rent/own, missing values ok)
        - 'unit_id' (int, '-1'-filled, corresponds to index of
          'residential_units' table)

    Results
    -------
    - adds following column to 'residential_units' table:
        - 'tenure' (str either rent/own, non-missing)
    """

    # abbreviate for brevity
    units = residential_units
    hh = households

    # tenure comes from PUMS - this used to have values of 1 and 2 but
    # these values are now mapped to the string 'own' and 'rent' for clarity

    units['tenure'] = np.nan
    own = hh[(hh.tenure == 'own') & (hh.unit_id != -1)].unit_id.values
    rent = hh[(hh.tenure == 'rent') & (hh.unit_id != -1)].unit_id.values
    units.loc[own, 'tenure'] = 'own'
    units.loc[rent, 'tenure'] = 'rent'

    print("Init unit tenure assignment: %d%% owner occupied, %d%% unfilled" %
          (round(len(units[units.tenure == 'own'])*100 /
           len(units[units.tenure.notnull()])),
           round(len(units[units.tenure.isnull()])*100 / len(units))))

    # Fill remaining units with random tenure assignment
    # TO DO: Make this weighted by existing allocation, rather than 50/50
    unfilled = units[units.tenure.isnull()].index
    units.loc[unfilled, 'tenure'] = \
        pd.Series(['rent', 'own']).sample(len(unfilled), replace=True).values

    return residential_units


@orca.step()
def initialize_residential_units(store):
    # this is assumed to run as preprocessing step, after the other
    # preprocessing steps - thus we need to get the data from the hdf rather
    # than from the orca tables - I contemplated putting this code in the
    # preprocessing.py module, but in the end I wanted to keep the residential
    # units code together, and also I wanted the github diff to show how few
    # lines actually changed here I'm not editing code - just changing where
    # this code runs
    households = store['households_preproc']
    buildings = store['buildings_preproc']

    # fan out buildings into units
    units = _create_empty_units(buildings)

    # put households into units based on the building id
    households = match_households_to_units(households, units)

    # then assign tenure to units based on the households in them
    units = assign_tenure_to_units(units, households)

    # write to the hdfstore
    store['households_preproc'] = households
    store['residential_units_preproc'] = units


@orca.step()
def load_rental_listings():
    """
    This initialization step loads the Craigslist rental listings data for
    hedonic estimation. Not needed for simulation.

    Data expectations
    -----------------
    - injectable 'net' that can provide 'node_id' and 'tmnode_id' from
      lat-lon coordinates
    - some way to get 'zone_id' (currently using parcels table)
    - 'sfbay_craigslist.csv' file

    Results
    -------
    - creates new 'craigslist' table with the following columns:
        - 'price' (int, may be missing)
        - 'sqft_per_unit' (int, may be missing)
        - 'price_per_sqft' (float, may be missing)
        - 'bedrooms' (int, may be missing)
        - 'neighborhood' (string, ''-filled)
        - 'node_id' (int, may be missing, corresponds to index of 'nodes')
        - 'tmnode_id' (int, may be missing, corresponds to index of 'tmnodes')
        - 'zone_id' (int, may be missing, corresponds to index of 'zones')
    - adds broadcasts linking 'craigslist' to 'nodes', 'tmnodes', 'logsums'
    """
    @orca.table('craigslist', cache=True)
    def craigslist():
        df = pd.read_csv(os.path.join(misc.data_dir(), "sfbay_craigslist.csv"))
        net = orca.get_injectable('net')
        df['node_id'] = net['walk'].get_node_ids(df['lon'], df['lat'])
        df['tmnode_id'] = net['drive'].get_node_ids(df['lon'], df['lat'])
        # fill nans -- missing bedrooms are mostly studio apts
        df['bedrooms'] = df.bedrooms.replace(np.nan, 1)
        df['neighborhood'] = df.neighborhood.replace(np.nan, '')
        return df

    # Is it simpler to just do this in the table definition since it
    # is never updated?
    @orca.column('craigslist', 'zone_id', cache=True)
    def zone_id(craigslist, parcels):
        return misc.reindex(parcels.zone_id, craigslist.node_id)

    orca.broadcast('nodes', 'craigslist', cast_index=True, onto_on='node_id')
    orca.broadcast('tmnodes', 'craigslist', cast_index=True,
                   onto_on='tmnode_id')
    orca.broadcast('zones', 'craigslist', cast_index=True, onto_on='zone_id')
    orca.broadcast('logsums', 'craigslist', cast_index=True, onto_on='zone_id')
    return


###############################################################################
#
# (2) UAL ORCA STEPS FOR DATA MODEL MAINTENANCE
#
###############################################################################


@orca.step()
def reconcile_placed_households(households, residential_units):
    """
    This data maintenance step keeps the building/unit/household correspondence
    up to date by reconciling placed households.

    In the current data model, households should have both a 'building_id' and
    'unit_id' when they have been matched with housing. But the existing HLCM
    models assign only a 'unit_id', so this model step updates the building
    id's accordingly.

    Data expectations
    -----------------
    - 'households' table has the following columns:
        - index 'household_id'
        - 'unit_id' (int, '-1'-filled)
        - 'building_id' (int, '-1'-filled)
    - 'residential_units' table has the following columns:
        - index 'unit_id'
        - 'building_id' (int, non-missing, corresponds to index of the
          'buildings' table)

    Results
    -------
    - updates the 'households' table:
        - 'building_id' updated where it was -1 but 'unit_id' wasn't
    """

    # Verify initial data characteristics

    # ot.assert_orca_spec(OrcaSpec('',
    #     TableSpec('households',
    #         ColumnSpec('household_id', primary_key=True),
    #         ColumnSpec('unit_id', foreign_key='residential_units.unit_id',
    #                    missing_val_coding=-1),
    #         ColumnSpec('building_id', foreign_key='buildings.building_id',
    #                    missing_val_coding=-1)),
    #     TableSpec('residential_units',
    #         ColumnSpec('unit_id', primary_key=True),
    #         ColumnSpec('building_id', foreign_key='buildings.building_id',
    #                    missing=False))))

    hh = households.to_frame(['unit_id', 'building_id'])
    hh.index.rename('household_id', inplace=True)
    hh = hh.reset_index()
    print("hh columns: %s" % hh.columns)

    # hh.index.name='household_id'
    units = residential_units.to_frame(['building_id']).reset_index()

    # Filter for households missing a 'building_id' but not a 'unit_id'
    hh = hh[(hh.building_id == -1) & (hh.unit_id != -1)]

    # Join building id's to the filtered households, using mapping
    # from the units table
    hh = hh.drop('building_id', axis=1)
    hh = pd.merge(hh, units, on='unit_id', how='left').\
        set_index('household_id')
    print("hh index.names: %s" % hh.index.names)

    print("%d movers updated" % len(hh))
    households.update_col_from_series('building_id',
                                      hh.building_id, cast=True)

    # Verify final data characteristics
    '''
    ot.assert_orca_spec(OrcaSpec(
        '', TableSpec(
            'households',
            ColumnSpec('building_id', foreign_key='buildings.building_id',
                       missing_val_coding=-1))))
    '''


@orca.step()
def reconcile_unplaced_households(households):
    """
    This data maintenance step keeps the building/unit/household
    correspondence up to date by reconciling unplaced households.

    In the current data model, households should have both a 'building_id'
    and 'unit_id' of -1 when they are not matched with housing. But sometimes
    only of these is set when households are created or unplaced. If households
    have been unplaced from buildings, this model step unplaces them from units
    as well. Or if they have been unplaced from units, it unplaces them from
    buildings.

    Data expectations
    -----------------
    - 'households' table has an index, and these columns:
        - 'unit_id' (int, '-1'-filled)
        - 'building_id' (int, '-1'-filled)

    Results
    -------
    - updates the 'households' table:
        - 'unit_id'='building_id'=-1 for the superset of rows where either
          column initially had this vaue
    """

    # Verify initial data characteristics
    '''
    ot.assert_orca_spec(OrcaSpec(
        '',
        TableSpec(
            'households',
            ColumnSpec('unit_id', numeric=True, missing_val_coding=-1),
            ColumnSpec('building_id', numeric=True, missing_val_coding=-1))))
    '''

    def _print_status():
        print("Households not in a unit: %d" %
              (households.unit_id == -1).sum())
        print("Househing missing a unit: %d" %
              households.unit_id.isnull().sum())
        print("Households not in a building: %d" %
              (households.building_id == -1).sum())
        print("Househing missing a building: %d" %
              households.building_id.isnull().sum())

    _print_status()
    print("Reconciling unplaced households...")
    hh = households.to_frame(['building_id', 'unit_id'])

    # Get indexes of households unplaced in buildings or in units
    bldg_unplaced = pd.Series(-1, index=hh[hh.building_id == -1].index)
    unit_unplaced = pd.Series(-1, index=hh[hh.unit_id == -1].index)

    # Update those households to be fully unplaced
    households.update_col_from_series('building_id', unit_unplaced, cast=True)
    households.update_col_from_series('unit_id', bldg_unplaced, cast=True)
    _print_status()

    # Verify final data characteristics
    '''
    ot.assert_orca_spec(OrcaSpec(
        '',
        TableSpec(
            'households',
            ColumnSpec('unit_id', foreign_key='residential_units.unit_id',
                       missing_val_coding=-1),
            ColumnSpec('building_id', foreign_key='buildings.building_id',
                       missing_val_coding=-1))))
    '''


@orca.step()
def remove_old_units(buildings, residential_units):
    """
    This data maintenance step removes units whose building_ids no longer
    exist.

    If new buildings have been created that re-use prior building_ids, we
    would fail to remove the associated units. Hopefully new buidlings do
    not duplicate prior ids, but this needs to be verified!

    Data expectations
    -----------------
    - 'buildings' table has an index that serves as its identifier
    - 'residential_units' table has a column 'building_id' corresponding
      to the index of the 'buildings' table

    Results
    -------
    - removes rows from the 'residential_units' table if their 'building_id'
      no longer
      exists in the 'buildings' table
    """

    # Verify initial data characteristics
    '''
    ot.assert_orca_spec(OrcaSpec(
        '',
        TableSpec(
            'buildings', ColumnSpec('building_id', primary_key=True)),
        TableSpec(
            'residential_units', ColumnSpec('building_id', numeric=True))))
    '''

    units = residential_units.to_frame(residential_units.local_columns)
    current_units = units[units.building_id.isin(buildings.index)]

    print("Removing %d units from %d buildings that no longer exist" %
          ((len(units) - len(current_units)),
           (len(units.groupby('building_id')) -
            len(current_units.groupby('building_id')))))

    orca.add_table('residential_units', current_units)

    # Verify final data characteristics
    '''
    ot.assert_orca_spec(OrcaSpec(
        '',
        TableSpec(
            'residential_units',
            ColumnSpec('building_id', foreign_key='buildings.building_id'))))
    '''


@orca.step()
def initialize_new_units(buildings, residential_units):
    """
    This data maintenance step initializes units for buildings that have been
    newly created, conforming to the data requirements of the
    'residential_units' table.

    Data expectations
    -----------------
    - 'buildings' table has the following columns:
        - index that serves as its identifier
        - 'residential_units' (int, count of units in building)
    - 'residential_units' table has the following columns:
        - index named 'unit_id' that serves as its identifier
        - 'building_id' corresponding to the index of the 'buildings' table

    Results
    -------
    - extends the 'residential_units' table, following the same schema as the
      'initialize_residential_units' model step
    """

    # Verify initial data characteristics
    '''
    ot.assert_orca_spec(OrcaSpec(
        '',
        TableSpec(
            'buildings',
            ColumnSpec('building_id', primary_key=True),
            ColumnSpec('residential_units', min=0)),
        TableSpec(
            'residential_units',
            ColumnSpec('unit_id', primary_key=True),
            ColumnSpec('building_id', foreign_key='buildings.building_id'))))
    '''

    old_units = residential_units.to_frame(residential_units.local_columns)
    bldgs = buildings.to_frame(['residential_units', 'deed_restricted_units'])

    # Filter for residential buildings not currently represented in
    # the units table
    new_bldgs = bldgs[~bldgs.index.isin(old_units.building_id)]
    new_bldgs = new_bldgs[new_bldgs.residential_units > 0]

    # Create new units, merge them, and update the table
    new_units = _create_empty_units(new_bldgs)
    all_units = dev.merge(old_units, new_units)
    all_units.index.name = 'unit_id'

    print("Creating %d residential units for %d new buildings" %
          (len(new_units), len(new_bldgs)))

    orca.add_table('residential_units', all_units)

    # Verify final data characteristics
    '''
    ot.assert_orca_spec(
        OrcaSpec('', TableSpec(
            'residential_units',
            ColumnSpec('unit_id', primary_key=True))))
    '''


@orca.step()
def assign_tenure_to_new_units(residential_units, households, settings):
    """
    This data maintenance step assigns tenure to new residential units.
    Tenure is determined by comparing the fitted sale price and fitted
    rent from the hedonic models, with rents adjusted to price-equivalent
    terms using a cap rate.

    We may want to make this more sophisticated in the future, or at least
    stochastic. Also, it might be better to do this assignment based on the
    zonal average prices and rents following supply/demand equilibration.

    Data expectations
    -----------------
    - 'residential_units' table has the following columns:
        - 'tenure' (str with values 'rent' and 'own', may be missing)
        - 'unit_residential_price' (float, non-missing)
        - 'unit_residential_rent' (float, non-missing)

    Results
    -------
    - fills missing values of 'tenure'
    """

    # Verify initial data characteristics
    '''
    # XXX can we restrict tenure to rent/own?
    ot.assert_orca_spec(
        OrcaSpec('', TableSpec(
            'residential_units',
            ColumnSpec('tenure', missing_val_coding=np.nan),
            ColumnSpec('unit_residential_price', min=0),
            ColumnSpec('unit_residential_rent', min=0))))
    '''

    cols = ['tenure', 'unit_residential_price', 'unit_residential_rent',
            'vacant_units']
    units = residential_units.to_frame(cols)

    # Filter for units that are missing a tenure assignment
    units = units[~units.tenure.isin(['own', 'rent'])]

    # Convert monthly rent to equivalent sale price
    cap_rate = settings.get('cap_rate')
    units['unit_residential_rent'] = \
        units.unit_residential_rent * 12 / cap_rate

    # Assign tenure based on higher of price or adjusted rent
    rental_units = (units.unit_residential_rent > units.unit_residential_price)
    units.loc[~rental_units, 'tenure'] = 'own'
    units.loc[rental_units, 'tenure'] = 'rent'
    units = unplaced_adjustment(households, units)

    print("Adding tenure assignment to %d new residential units" % len(units))
    print(units.describe())

    residential_units.update_col_from_series(
        'tenure', units.tenure, cast=True)


def unplaced_adjustment(households, units):
    """
    Modifies tenure assignment to new units, so that it is not only based on
    the highest value between sale price and rent (converted to equivalent),
    but also considers the minimum number of units required by tenure category
    to accommodate existing unplaced households.

    Parameters
    ----------
    households : Orca table
    units : pd.DataFrame with initial tenure assignments

    Returns
    -------
    units : pd.DataFrame with adjusted tenure assignments

    """
    hh = households.to_frame(['unit_id', 'building_id', 'tenure'])
    vacant_units = units[units['vacant_units'] > 0].copy()
    vacant_units['rent_over_price'] = vacant_units['unit_residential_rent'] \
        / vacant_units['unit_residential_price']
    vacant_units['price_over_rent'] = vacant_units['unit_residential_price'] \
        / vacant_units['unit_residential_rent']
    min_new = {}

    for tenure in ['own', 'rent']:
        units_tenure = vacant_units[vacant_units['tenure'] == tenure]
        vacant_units_tenure = units_tenure.vacant_units.sum()
        unplaced_hh = hh[(hh['tenure'] == tenure) & (hh['unit_id'] == -1)]
        unplaced_hh = len(unplaced_hh.index)
        min_new[tenure] = max(unplaced_hh - vacant_units_tenure, 0)

    complement = {'own': 'rent', 'rent': 'own'}
    price = {'own': 'price_over_rent', 'rent': 'rent_over_price'}

    for tenure in ['own', 'rent']:
        units_tenure = vacant_units[vacant_units['tenure'] == tenure]
        units_comp = vacant_units[vacant_units['tenure'] == complement[tenure]]

        if (min_new[complement[tenure]] < len(units_comp.index)) & \
                (min_new[tenure] > len(units_tenure.index)):
            missing_units = min_new[tenure] - len(units_tenure.index)
            extra_units = len(units_comp.index) - min_new[complement[tenure]]
            extra_units = int(min(missing_units, extra_units))
            extra_units = units_comp.nlargest(extra_units, price[tenure])
            units.loc[extra_units.index, 'tenure'] = tenure

    return units


@orca.step()
def save_intermediate_tables(households, buildings, parcels,
                             jobs, zones, year):
    """
    This orca step saves intermediate versions of data tables, for developing
    visualization proofs of concept.
    """
    filename = 'baus_' + str(year) + '.h5'
    for table in [households, buildings, parcels, jobs, zones]:
        table.to_frame().to_hdf(filename, table.name)


###############################################################################
#
# (3) UAL ORCA STEPS FOR SIMULATION LOGIC
#
###############################################################################


# have to define this here because urbansim_defaults incorrectly calls the
# outcome variable non_residential_price
@orca.step('nrh_simulate')
def nrh_simulate(buildings, aggregations, nrh_config):
    return utils.hedonic_simulate(nrh_config, buildings, aggregations,
                                  "non_residential_rent")


@orca.step()
def rsh_estimate(buildings, aggregations):
    return utils.hedonic_estimate("rsh.yaml", buildings, aggregations)


@orca.step('rrh_estimate')
def rrh_estimate(craigslist, aggregations):
    """
    This model step estimates a residental rental hedonic using
    craigslist listings.

    Data expectations
    -----------------
    - 'craigslist' table and others, as defined in the yaml config
    """
    return utils.hedonic_estimate(cfg='rrh.yaml',
                                  tbl=craigslist,
                                  join_tbls=aggregations)


def _mtc_clip(table, col_name, settings, price_scale=1):
    # This is included to match the MTC hedonic model steps, with 'price_scale'
    # adjusting the clip bounds from price to monthly rent if needed.

    if "rsh_simulate" in settings:
        low = float(settings["rsh_simulate"]["low"]) * price_scale
        high = float(settings["rsh_simulate"]["high"]) * price_scale
        table.update_col(col_name, table[col_name].clip(low, high))
        print("Clipping produces\n", table[col_name].describe())


@orca.step()
def rsh_simulate(residential_units, aggregations, settings, rsh_config):
    """
    This uses the MTC's model specification from rsh.yaml, but
    generates unit-level price predictions rather than building-level.

    Data expectations
    -----------------
    - tk
    """
    utils.hedonic_simulate(cfg=rsh_config,
                           tbl=residential_units,
                           join_tbls=aggregations,
                           out_fname='unit_residential_price')

    _mtc_clip(residential_units, 'unit_residential_price', settings)
    return


@orca.step()
def rrh_simulate(residential_units, aggregations, settings, rrh_config):
    """
    This uses an altered hedonic specification to generate
    unit-level rent predictions.

    Data expectations
    -----------------
    - tk
    """
    utils.hedonic_simulate(cfg=rrh_config,
                           tbl=residential_units,
                           join_tbls=aggregations,
                           out_fname='unit_residential_rent')

    _mtc_clip(residential_units, 'unit_residential_rent',
              settings, price_scale=0.05/12)
    return


@orca.step()
def households_relocation(households, settings):
    """
    This model step randomly assigns households for relocation, using
    probabilities that depend on their tenure status.

    Data expectations
    -----------------
    - 'households' table has following columns:
        - 'tenure' (str either rent/own, non-missing)
        - 'building_id' (int, '-1'-filled, corredponds to index of
          'buildings' table
        - 'unit_id' (int, '-1'-filled, corresponds to index of
          'residential_units' table
    - 'settings.yaml' has:
        - 'relocation_rates' as specified in RelocationModel() documentation

    Results
    -------
    - assigns households for relocation by setting their 'building_id' and
      'unit_id' to -1
    """

    # Verify expected data characteristics
    '''
    # XXX restrict tenure to rent/own
    ot.assert_orca_spec(
        OrcaSpec('', TableSpec(
            'households',
            ColumnSpec('tenure', missing=False),
            ColumnSpec('building_id', numeric=True, missing_val_coding=-1),
            ColumnSpec('unit_id', numeric=True, missing_val_coding=-1))))
    '''

    rates = pd.DataFrame.from_dict(settings['relocation_rates'])

    print("Total agents: %d" % len(households))
    print("Total currently unplaced: %d" % (households.unit_id == -1).sum())
    print("Assigning for relocation...")

    # Initialize model, choose movers, and un-place them from buildings
    # and units
    m = RelocationModel(rates)
    mover_ids = m.find_movers(households.to_frame(['unit_id', 'tenure']))
    households.update_col_from_series(
        'building_id', pd.Series(-1, index=mover_ids), cast=True)
    households.update_col_from_series(
        'unit_id', pd.Series(-1, index=mover_ids), cast=True)

    print("Total currently unplaced: %d" % (households.unit_id == -1).sum())
    return


@orca.step()
def hlcm_owner_estimate(households, residential_units, aggregations):
    return utils.lcm_estimate(cfg="hlcm_owner.yaml",
                              choosers=households,
                              chosen_fname="unit_id",
                              buildings=residential_units,
                              join_tbls=aggregations)


@orca.step()
def hlcm_renter_estimate(households, residential_units, aggregations):
    return utils.lcm_estimate(cfg="hlcm_renter.yaml",
                              choosers=households,
                              chosen_fname="unit_id",
                              buildings=residential_units,
                              join_tbls=aggregations)


# use one core hlcm for the hlcms below, with different yaml files
def hlcm_simulate(households, residential_units, aggregations,
                  settings, yaml_name, equilibration_name):

    return utils.lcm_simulate(cfg=yaml_name,
                              choosers=households,
                              buildings=residential_units,
                              join_tbls=aggregations,
                              out_fname='unit_id',
                              supply_fname='num_units',
                              vacant_fname='vacant_units',
                              enable_supply_correction=settings.get(
                                equilibration_name, None),
                              cast=True)


@orca.step()
def hlcm_owner_simulate(households, residential_units,
                        aggregations, settings,
                        hlcm_owner_config):

    # Note that the submarket id (zone_id) needs to be in the table of
    # alternatives, for supply/demand equilibration, and needs to NOT be in the
    # choosers table, to avoid conflicting when the tables are joined

    # Pre-filter the alternatives to avoid over-pruning (PR 103)
    correct_alternative_filters_sample(residential_units, households, 'own')

    hlcm_simulate(orca.get_table('own_hh'), orca.get_table('own_units'),
                  aggregations, settings, hlcm_owner_config,
                  'price_equilibration')

    update_unit_ids(households, 'own')


@orca.step()
def hlcm_owner_lowincome_simulate(households, residential_units,
                                  aggregations, settings,
                                  hlcm_owner_lowincome_config):

    return hlcm_simulate(households, residential_units, aggregations,
                         settings, hlcm_owner_lowincome_config,
                         'price_equilibration')


@orca.step()
def hlcm_renter_simulate(households, residential_units, aggregations,
                         settings, hlcm_renter_config):

    # Pre-filter the alternatives to avoid over-pruning (PR 103)
    correct_alternative_filters_sample(residential_units, households, 'rent')

    hlcm_simulate(orca.get_table('rent_hh'), orca.get_table('rent_units'),
                  aggregations, settings, hlcm_renter_config,
                  'rent_equilibration')

    update_unit_ids(households, 'rent')


def correct_alternative_filters_sample(residential_units, households, tenure):
    """
    Creates modified versions of the alternatives and choosers Orca tables
    (residential units and households), so that the parameters that will be
    given to the hlcm_simulate() method are already filtered with the
    alternative filters defined in the hlcm_owner and hlcm_renter yaml files.

    Parameters
    ----------
    residential_units: Orca table
    households: Orca table
    tenure: str, 'rent' or 'own'

    Returns
    -------
    None. New tables of residential units and households by tenure segment
    are registered in Orca, with broadcasts linking them to each other and
    to the 'buildings' table.

    """
    units = residential_units.to_frame()
    units_tenure = units[units.tenure == tenure]
    units_name = tenure + '_units'
    orca.add_table(units_name, units_tenure, cache=True, cache_scope='step')

    hh = households.to_frame()
    hh_tenure = hh[hh.tenure == tenure]
    hh_name = tenure + '_hh'
    orca.add_table(hh_name, hh_tenure, cache=True, cache_scope='step')

    orca.broadcast('buildings', units_name,
                   cast_index=True, onto_on='building_id')
    orca.broadcast(units_name, hh_name, cast_index=True, onto_on='unit_id')


def update_unit_ids(households, tenure):
    """
    After running the hlcm simulation for a given tenure (own or rent), this
    function retrieves the new unit_id values from the own_hh or rent_hh
    Orca tables as applicable. It then updates the general households table
    with the new unit ids that were selected by unplaced households.

    Parameters
    ----------
    households : Orca table
    tenure : str, 'rent' or 'own'

    Returns
    -------
    None. unit_id column gets updated in the households table.

    """
    unit_ids = households.to_frame(['unit_id'])
    updated = orca.get_table(tenure+'_hh').to_frame(['unit_id'])
    unit_ids.loc[unit_ids.index.isin(updated.index),
                 'unit_id'] = updated['unit_id']
    households.update_col_from_series('unit_id', unit_ids.unit_id, cast=True)


@orca.step()
def hlcm_renter_lowincome_simulate(households, residential_units, aggregations,
                                   settings, hlcm_renter_lowincome_config):
    return hlcm_simulate(households, residential_units, aggregations,
                         settings, hlcm_renter_lowincome_config,
                         'rent_equilibration')


# this opens the yaml file, deletes the predict filters and writes it to the
# out name - since the alts don't have a filter, all hhlds should be placed
def drop_predict_filters_from_yaml(in_yaml_name, out_yaml_name):
    fname = misc.config(in_yaml_name)
    cfg = yaml.load(open(fname))
    cfg["alts_predict_filters"] = None
    open(misc.config(out_yaml_name), "w").write(yaml.dump(cfg))


# see comment above - these hlcms ignore tenure in the alternatives and so
# place households as long as there are empty units - this should only run
# in the final year
@orca.step()
def hlcm_owner_simulate_no_unplaced(households, residential_units,
                                    year, final_year,
                                    aggregations, settings,
                                    hlcm_owner_config,
                                    hlcm_owner_no_unplaced_config):

    # only run in the last year, but make sure to run before summaries
    if year != final_year:
        return

    drop_predict_filters_from_yaml(
        hlcm_owner_config,
        hlcm_owner_no_unplaced_config)

    return hlcm_simulate(households, residential_units, aggregations,
                         settings, hlcm_owner_no_unplaced_config,
                         "price_equilibration")


@orca.step()
def hlcm_renter_simulate_no_unplaced(households, residential_units,
                                     year, final_year,
                                     aggregations, settings,
                                     hlcm_renter_config,
                                     hlcm_renter_no_unplaced_config):

    # only run in the last year, but make sure to run before summaries
    if year != final_year:
        return

    drop_predict_filters_from_yaml(
        hlcm_renter_config,
        hlcm_renter_no_unplaced_config)

    return hlcm_simulate(households, residential_units, aggregations,
                         settings, hlcm_renter_no_unplaced_config,
                         "rent_equilibration")


@orca.step()
def balance_rental_and_ownership_hedonics(households, settings,
                                          residential_units):
    hh_rent_own = households.tenure.value_counts()
    unit_rent_own = residential_units.tenure.value_counts()

    # keep these positive by not doing a full vacancy rate - just divide
    # the number of households by the number of units for each tenure
    owner_utilization = hh_rent_own.own / float(unit_rent_own.own)
    renter_utilization = hh_rent_own.rent / float(unit_rent_own.rent)

    print("Owner utilization = %.3f" % owner_utilization)
    print("Renter utilization = %.3f" % renter_utilization)

    utilization_ratio = renter_utilization / owner_utilization
    print("Ratio of renter utilization to owner utilization = %.3f" %
          utilization_ratio)

    if "original_cap_rate" not in settings:
        settings["original_cap_rate"] = settings["cap_rate"]

    factor = 1.4
    # move the ratio away from zero to have more of an impact
    if utilization_ratio < 1.0:
        utilization_ratio /= factor
    elif utilization_ratio > 1.0:
        utilization_ratio *= factor
    print("Modified ratio = %.3f" % utilization_ratio)

    # adjust the cap rate based on utilization ratio - higher ratio
    # here means renter utilization is higher than owner utilization
    # meaning rent price should go up meaning cap rate should go down
    # FIXME these might need a parameter to spread or narrow the impact
    settings["cap_rate"] = settings["original_cap_rate"] /\
        utilization_ratio

    print("New cap rate = %.2f" % settings["cap_rate"])
