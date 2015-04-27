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
res_codes = {'single': [],
             'multi': [],
             'mixed': []}
exempt_codes = []


## Register input tables.


tf = TableFrame(staging.FIXME, index_col='FIXME')
sim.add_table('parcels_in', tf, copy_col=False)


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
    pass


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(code='FIXME'):
    return code


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_res_type(land_use_type_id, res_codes)


@out
def land_value(value='FIXME'):
    return value


@out
def improvement_value(value='FIXME'):
    return value


@out
def year_assessed(year='FIXME'):
    year.replace(0, np.nan, inplace=True)
    return year


@out
def year_built(year='FIXME'):
    year.replace(0, np.nan, inplace=True)
    return year


@out
def building_sqft(sqft='FIXME'):
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
def sqft_per_unit(building_sqft='parcels_out.building_sqft',
                  non_residential_sqft='parcels_out.non_residential_sqft',
                  residential_units='parcels_out.residential_units'):
    return utils.get_sqft_per_unit(building_sqft, non_residential_sqft,
                                   residential_units)


@out
def stories(stories='FIXME'):
    return stories


@out
def tax_exempt(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_tax_exempt(land_use_type_id, exempt_codes)


## Export back to database.


@sim.model()
def export_FIXME(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_FIXME', schema=staging)

sim.run(['export_FIXME'])
