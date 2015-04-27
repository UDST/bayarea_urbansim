import numpy as np
import pandas as pd
from spandex import TableLoader, TableFrame
from spandex.io import df_to_db
import urbansim.sim.simulation as sim

import utils


loader = TableLoader()
staging = loader.tables.staging


## Assumptions.


exempt_codes = []


## Register input tables.


# Alternate index column is "mapblklot", but it was not unique for
# parcels in Treasure Island.
tf = TableFrame(staging.parcels_sfr, index_col='blklot')
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
    return '075'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(code='parcels_in.landuse'):
    # Alternate inputs:
    # - "usetype"
    # - "mixeduse"
    code[(code == 'MISSING DATA') | (code == 'Missing Data')] = None
    return code


@out
def res_type(restype='parcels_in.restype'):
    return restype


@out
def land_value(value='parcels_in.landval'):
    # Alternate inputs:
    # - "lastsalepr"
    # - "assessedva"
    # - "estimatedv"
    return value.astype(float)


@out
def improvement_value(value='parcels_in.strucval'):
    # Alternate inputs:
    # - "lastsalepr"
    # - "assessedva"
    # - "estimatedv"
    return value.astype(float)


@out
def year_assessed(date='parcels_in.lastsale'):
    year = date.apply(lambda d: d.year if d else np.nan)
    year.replace([-1, 0], np.nan, inplace=True)
    return year


@out
def year_built(year='parcels_in.yrbuilt'):
    year.replace(0, np.nan, inplace=True)
    return year


@out
def building_sqft(sqft='parcels_in.bldgsqft'):
    # Alternate inputs:
    # - "lidarsqft"
    # - "high_sqft"
    # - "total_uses"
    # - "costarrba"
    # - "totalsqft"
    # - "netsqft"
    # May want to aggregate multiple inputs to avoid zeros that represent
    # missing data.
    return sqft.astype(float)


@out
def non_residential_sqft(sqft='parcels_in.commlsqft_'):
    # Alternate inputs:
    # - "commsqft"
    return sqft.astype(float)


@out
def residential_units(units='parcels_in.resunits'):
    return units
    
@out
def condo_identifier():
    code = ' '
    return code


@out
def sqft_per_unit(ressqft='parcels_in.ressqft',
                  residential_units='parcels_out.residential_units'):
    # Alternate inputs:
    # - "ressqft_ex"
    per_unit = 1. * ressqft / residential_units
    per_unit.replace(np.inf, np.nan, inplace=True)
    return per_unit


@out
def stories(stories='parcels_in.stories'):
    # Alternate inputs:
    # - "lidarstori": always zero
    return stories


@out
def tax_exempt(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_tax_exempt(land_use_type_id, exempt_codes)


## Export back to database.


@sim.model()
def export_sfr(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_sfr', schema=staging)

sim.run(['export_sfr'])
