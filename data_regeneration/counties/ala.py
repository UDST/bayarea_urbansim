import numpy as np
import pandas as pd
from spandex import TableLoader, TableFrame
from spandex.io import df_to_db
import urbansim.sim.simulation as sim

import utils


loader = TableLoader()
staging = loader.tables.staging


## Assumptions.


# Use codes were classified manually because the assessor classifications
# are meant for property tax purposes. These classifications should be
# reviewed and revised.
res_codes = {'single': ([1100] + range(1120, 1151) + range(1200, 1501) +
                        range(1900, 2000)),
             'multi': (range(600, 1100) + [1700] + range(2000, 3000) +
                       range(5000, 5300) + range(7000, 7701) + [7800]),
             'mixed': (range(3900, 4000) + [4101] + [4191] + [4240] +
                       [9401] + [9491])}
exempt_codes = range(1, 1000)


## Register input tables.


tf = TableFrame(staging.parcels_ala, index_col='apn_sort')
sim.add_table('parcels_in', tf, copy_col=False)


@sim.table(cache=True)
def ie670():
    filepath = \
        loader.get_path('built/parcel/2010/ala/assessor_nov10/IE670c.txt')
    df = pd.read_table(filepath, sep='\t', index_col=False, low_memory=False)
    df.set_index("Assessor's Parcel Number (APN) sort format", inplace=True)
    assert df.index.is_unique
    assert not df.index.hasnans()
    return df


@sim.table(cache=True)
def ie673():
    filepath = \
        loader.get_path('built/parcel/2010/ala/assessor_nov10/IE673c.txt')
    df = pd.read_table(filepath, sep='\t', index_col=False)
    df.set_index('APNsort', inplace=True)
    assert df.index.is_unique
    assert not df.index.hasnans()
    return df


## Register output table.


@sim.table(cache=True)
def parcels_out(parcels_in):
    index = pd.Series(parcels_in.index).dropna().unique()
    df = pd.DataFrame(index=index)
    df.index.name = 'apn'
    return df


## Register output columns.


out = sim.column('parcels_out', cache=True)


@out
def county_id():
    return '001'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(parcels_out, code='ie673.UseCode'):
    # Alternate inputs:
    # - "Use Code": from IE670, values are identical
    return code.reindex(parcels_out.index, copy=False).fillna(0).astype(int)


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_res_type(land_use_type_id, res_codes)


@out
def land_value(value='ie670.Land value'):
    # Alternate inputs:
    #  - "CLCA land value": less data available
    return value


@out
def improvement_value(value='ie670.Improvements value'):
    # Alternate inputs:
    #  - "CLCA improvements value": less data available
    return value


@out
def year_assessed(date='ie670.Last document date (CCYYMMDD)'):
    # Alternate inputs:
    # - "Last document prefix": not always numeric
    # - "Last document input date (CCYYMMDD)"
    # - "Property characteristic change date (CCYYMMDD)": from IE673
    date.replace(0, np.nan, inplace=True)
    return date.floordiv(10000)


@out
def year_built(year='ie673.YearBuilt'):
    return year.str.strip().replace('', np.nan).astype(float)


@out
def building_sqft(sqft='ie673.BldgArea'):
    return sqft


@out
def non_residential_sqft(building_sqft='parcels_out.building_sqft',
                         res_type='parcels_out.res_type',
                         residential_units='parcels_out.residential_units'):
    return utils.get_nonresidential_sqft(building_sqft, res_type,
                                         residential_units)


@out
def residential_units(tot_units='ie673.Units',
                      res_type='parcels_out.res_type'):
    return utils.get_residential_units(tot_units, res_type)
    
@out
def condo_identifier():
    code = ' '
    return code


@out
def sqft_per_unit(building_sqft='parcels_out.building_sqft',
                  non_residential_sqft='parcels_out.non_residential_sqft',
                  residential_units='parcels_out.residential_units'):
    return utils.get_sqft_per_unit(building_sqft, non_residential_sqft,
                                   residential_units)


@out
def stories(stories='ie673.Stories'):
    # 1 story = 10 Alameda County stories.
    return 0.1 * stories


@out
def tax_exempt(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_tax_exempt(land_use_type_id, exempt_codes)


## Export back to database.


@sim.model()
def export_ala(parcels_out):
    df = parcels_out.to_frame()

    # Cast "land_use_type_id" to string for compatibility with other counties.
    df['land_use_type_id'] = df.land_use_type_id.astype(str)

    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_ala', schema=staging)

sim.run(['export_ala'])
