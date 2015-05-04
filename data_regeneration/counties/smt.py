import os
import string
import subprocess

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
res_codes = {'single': ['01', '51', '52', '53'],
             'multi': [string.zfill(i, 2) for i in
                       range(2, 6) + range(7, 10) + range(89, 99)],
             'mixed': []}
exempt_codes = []


## Register input tables.


tf = TableFrame(staging.parcels_smt, index_col='apn')
sim.add_table('parcels_in', tf, copy_col=False)


@sim.table(cache=True)
def roll():
    mdb = loader.get_path(
        'built/parcel/2010/smt/Property Characteristics/ASSESSOR_ROLL.mdb'
    )
    csv = os.path.splitext(mdb)[0] + '.csv'

    if not os.path.exists(csv):
        with open(csv, 'w') as f:
            # Export of MS Access database requires mdbtools.
            subprocess.check_call(['mdb-export', mdb, '2009_ROLL'], stdout=f)

    df = pd.read_csv(csv, dtype={'APN': str}, low_memory=False)

    # Deduplicate by taking the last record, sorted by sequence ID ("SEQ").
    # Alternatively, could sort by date ("DOR") for similar results.
    df.sort('SEQ', ascending=True, inplace=True)
    df.drop_duplicates('APN', take_last=True, inplace=True)

    df.set_index('APN', inplace=True)
    assert df.index.is_unique
    assert not df.index.hasnans()
    return df


@sim.table(cache=True)
def situs():
    csv = loader.get_path(
        'built/parcel/2010/smt/Property Characteristics/SITUS_SNPSHT.csv'
    )
    df = pd.read_csv(csv, dtype={'APN': str}, low_memory=False)
    df = df[df.APN.notnull()]
    df.set_index('APN', inplace=True)
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
    return '081'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(code='situs.USE_CODE'):
    return code


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_res_type(land_use_type_id, res_codes)


@out
def land_value(value='roll.ASSLAND'):
    # Alternate inputs:
    # - "TEMPLAND"
    return value


@out
def improvement_value(value='roll.ASSIMPS'):
    # Alternate inputs:
    # - "TEMPIMPS"
    return value


@out
def year_assessed(date='roll.DOR'):
    # Assumed that "DOR" represents date of record or similar.
    date.replace(0, np.nan, inplace=True)
    return date.floordiv(10000)


@out
def year_built():
    return np.nan


@out
def building_sqft():
    # Workaround since np.nan would raise an exception when injecting below.
    return pd.Series()


@out
def non_residential_sqft(building_sqft='parcels_out.building_sqft',
                         res_type='parcels_out.res_type',
                         residential_units='parcels_out.residential_units'):
    return utils.get_nonresidential_sqft(building_sqft, res_type,
                                         residential_units)


@out
def residential_units(res_type='parcels_out.res_type'):
    return utils.get_residential_units(tot_units=pd.Series(),
                                       res_type=res_type)
                                       
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
def export_smt(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df.res_type[df.res_type.isnull()] = ''
    df_to_db(df, 'attributes_smt', schema=staging)

sim.run(['export_smt'])
