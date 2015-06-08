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
res_codes = {'single': [str(i) for i in range(11, 17)] + ['19'],
             'multi': ['10', '17', '18', '61', '88'] +
                      [str(i) for i in range(20, 30)],
             'mixed': ['48', '89']}
exempt_codes = []


## Register input tables.


tf = TableFrame(staging.parcels_cnc_pt)
sim.add_table('parcels_in', tf, copy_col=False)


## Register intermediate table and columns.

# The purpose of this intermediate table is to compute certain fields,
# like non_residential_sqft and residential_units, before grouping together
# records with the same parc_py_id. Thus, single-family condominium units
# would each be assumed to have one residential unit, and this count would
# be summed when grouping later.


@sim.table()
def parcels_in2(parcels_in):
    return pd.DataFrame(index=parcels_in.index)


in2 = sim.column('parcels_in2', cache=True)


@in2
def res_type2(land_use_type_id='parcels_in.use_code'):
    return utils.get_res_type(land_use_type_id, res_codes)

@in2
def building_sqft2(bldg_sqft='parcels_in.bldg_sqft', tla='parcels_in.tla'):
    # Alternate inputs:
    # - "NET_RNT_AR"
    sqft = pd.concat([bldg_sqft, tla])
    return sqft.groupby(level=0).max()


@in2
def non_residential_sqft2(building_sqft='parcels_in2.building_sqft2',
                          res_type='parcels_in2.res_type2',
                          residential_units='parcels_in2.residential_units2'):
    return utils.get_nonresidential_sqft(building_sqft, res_type,
                                         residential_units)
                                         

@in2
def residential_units2(tot_units='parcels_in.units',
                       res_type='parcels_in2.res_type2', land_use_type_id='parcels_in.use_code'):
                       
    units = pd.Series(index=res_type.index)
    tot_units = tot_units.reindex(units.index, copy=False)
    land_use_type_id = land_use_type_id.reindex(units.index, copy=False)

    # If not residential, assume zero residential units.
    units[res_type.isnull()] = 0

    # If single family residential, assume one residential unit.
    units[res_type == 'single'] = 1

    # If non-single residential, assume all units are all residential,
    # even if mixed-use.
    units[res_type == 'multi'] = tot_units
    units[res_type == 'mixed'] = tot_units

    # Note in the points layer, certain condos and PUDs (as denoted by lutype 29)
    # are split up into one record per residential unit with the unit column left
    # unpopulated or as 0.  In such cases, we assign a value of 1 so that when 
    # we group by parc_py_id, the resulting MF residential unit counts make sense,
    # e.g. in Rossmoor.
    units[land_use_type_id.isin(['29',])*np.logical_or((tot_units==0), tot_units.isnull())] = 1

    return units


## Register output table.


@sim.table(cache=True)
def parcels_out(parcels_in):
    # Rename "parc_py_id" to "apn" to satisfy unique constraint.
    # There are duplicate APNs that should be merged together, but
    index = parcels_in.parc_py_id.dropna().unique()
    df = pd.DataFrame(index=index)
    df.index.name = 'apn'
    return df


## Register output columns.


regroup = lambda c: c.groupby(tf.parc_py_id)
out = sim.column('parcels_out', cache=True)


@out
def county_id():
    return '013'


@out
def parcel_id_local(apn='parcels_in.apn'):
    return regroup(apn).first()


@out
def land_use_type_id(code='parcels_in.use_code'):
    return regroup(code).first()


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_res_type(land_use_type_id, res_codes)


@out
def land_value(value='parcels_in.land_value', sign='parcels_in.lnd_val_sn'):
    sign = sign.replace({'-': -1, '+': +1}).astype(float)
    value = value * sign
    return regroup(value).sum()


@out
def improvement_value(value='parcels_in.imp_val',
                      sign='parcels_in.imp_val_sn'):
    sign = sign.replace({'-': -1, '+': +1}).astype(float)
    value = value * sign
    return regroup(value).sum()


@out
def year_assessed(date='parcels_in.c_ded_dt'):
    # Assume that current deed date is date of assessment.
    year = date.astype(float).floordiv(10000)
    year.replace(0, np.nan, inplace=True)
    return regroup(year).median()


@out
def year_built(yr='parcels_in.yr_built', yr_hs='parcels_in.yr_hs_blt'):
    # Alternate inputs:
    # - "yr_blt_msc"
    year = pd.concat([yr, yr_hs]).astype(float)
    year.replace(0, np.nan, inplace=True)
    return regroup(year).median()


@out
def building_sqft(building_sqft='parcels_in2.building_sqft2'):
    return regroup(building_sqft).sum()


@out
def non_residential_sqft(non_residential_sqft=
                         'parcels_in2.non_residential_sqft2'):
    return regroup(non_residential_sqft).sum()


@out
def residential_units(parcels_in, residential_units='parcels_in2.residential_units2'):
    return regroup(residential_units).sum()
    
@out
def condo_identifier(address='parcels_in.s_str_nbr', street='parcels_in.s_str_nm', zipcode='parcels_in.s_zip'):
    address = regroup(address).first()
    street = regroup(street).first()
    zipcode = regroup(zipcode).first()
    code = address + street + zipcode
    code[code.isnull()] = ''
    #code[code == ''] = None
    return code


@out
def sqft_per_unit(building_sqft='parcels_out.building_sqft',
                  non_residential_sqft='parcels_out.non_residential_sqft',
                  residential_units='parcels_out.residential_units'):
    return utils.get_sqft_per_unit(building_sqft, non_residential_sqft,
                                   residential_units)


@out
def stories(stories='parcels_in.stories'):
    return regroup(stories).median()


@out
def tax_exempt(code='parcels_in.n_tax_code'):
    return regroup(code).first().notnull().astype(int)


## Export back to database.


@sim.model()
def export_cnc(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans
    df.res_type[df.res_type.isnull()] = ''
    df_to_db(df, 'attributes_cnc', schema=staging)

sim.run(['export_cnc'])
