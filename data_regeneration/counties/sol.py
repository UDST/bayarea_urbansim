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
# Residential status was classified using SELECT DISTINCT usecode, remark1.
res_codes = {'single': ['1000', '1500'],
             'multi': ['1100', '2000', '2100', '2700'],
             'mixed': []}
exempt_codes = []


## Register input tables.


tf = TableFrame(staging.parcels_sol)


@sim.table(cache=True)
def parcels_in():
    # Build DataFrame from subset of TableFrame columns.
    column_names = ['apn', 'usecode', 'valland', 'valimp', 'pdyrblt',
                    'pdareafst', 'pdareasnd']
    df = tf[column_names]

    # Deduplicate DataFrame by taking the last record, but this might
    # not be the right approach.
    df.drop_duplicates('apn', take_last=True, inplace=True)

    df.set_index('apn', inplace=True)
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
    return '095'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(code='parcels_in.usecode'):
    return code.astype(str)


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_res_type(land_use_type_id, res_codes)


@out
def land_value(value='parcels_in.valland'):
    return value


@out
def improvement_value(value='parcels_in.valimp'):
    return value


@out
def year_assessed():
    return np.nan


@out
def year_built(year='parcels_in.pdyrblt'):
    year.replace(0, np.nan, inplace=True)
    return year


@out
def building_sqft(sqft_fst='parcels_in.pdareafst',
                  sqft_snd='parcels_in.pdareasnd'):
    return sqft_fst + sqft_snd


@out
def non_residential_sqft(building_sqft='parcels_out.building_sqft',
                         res_type='parcels_out.res_type',
                         residential_units='parcels_out.residential_units'):
    return utils.get_nonresidential_sqft(building_sqft, res_type,
                                         residential_units)


@out
def residential_units(res_type='parcels_out.res_type'):
    units = pd.Series(index=res_type.index)

    # If single family residential, assume one residential unit.
    units[res_type == 'single'] = 1
    
    # If multi family residential, assume one residential unit for now too (leave further assumed additions to the imputation section).
    units[res_type == 'multi'] = 1

    return units
    
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
def stories():
    return np.nan


@out
def tax_exempt(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_tax_exempt(land_use_type_id, exempt_codes)


## Export back to database.


@sim.model()
def export_sol(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_sol', schema=staging)

sim.run(['export_sol'])
