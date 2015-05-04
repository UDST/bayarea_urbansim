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
res_codes = {'single': ['11', '12', '13', '14'],
             'multi': ['10', '15', '20', '21', '57'],
             'mixed': []}
exempt_codes = ['60', '61', '80']


## Register input tables.


tf = TableFrame(staging.parcels_mar, index_col='parcel')
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
    return '041'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(code='parcels_in.usecode'):
    # Alternate inputs:
    # - "landuse"
    # - "county_lu"
    # - "existlu"
    # - "potlu"
    return code


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_res_type(land_use_type_id, res_codes)


@out
def land_value(value='parcels_in.colndval'):
    # Alternate inputs:
    # - "metlndval"
    # - "mettotval"
    # - "saleprice"
    # - "est05landv"
    return value


@out
def improvement_value(value='parcels_in.coimpval'):
    # Alternate inputs:
    # - "metimpval"
    # - "mettotval"
    # - "saleprice"
    # - "est05imprv"
    return value


@out
def year_assessed(year='parcels_in.saledate'):
    # Alternate inputs:
    # - "deeddate": often same as saledate
    # - "lastdate": often during year preceding baseyear
    # - "baseyear": less data available
    year = year.str.slice(0, 4).astype(float)
    year.replace(0, np.nan, inplace=True)
    return year


@out
def year_built(year='parcels_in.yearbuilt'):
    year = year.astype(float)
    year.replace(0, np.nan, inplace=True)
    return year


@out
def building_sqft(sqft='parcels_in.existsqft'):
    # Alternate inputs:
    # - "potsqft"
    # - "buildingsq"
    # - "e05tpbsqft"
    return sqft


@out
def non_residential_sqft(building_sqft='parcels_out.building_sqft',
                         res_type='parcels_out.res_type',
                         residential_units='parcels_out.residential_units'):
    return utils.get_nonresidential_sqft(building_sqft, res_type,
                                         residential_units)


@out
def residential_units(tot_units='parcels_in.exunits',
                      res_type='parcels_out.res_type'):
    # Alternate inputs:
    # - "liv_units": appears to include commercial units
    # - "potunits"
    # - "unitsnumbe"
    # - "b_units"
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
def stories(stories='parcels_in.storiesnum'):
    return stories.astype(float)


@out
def tax_exempt(land_use_type_id='parcels_out.land_use_type_id'):
    # "exemption" column is value of exemptions, which includes welfare.
    return utils.get_tax_exempt(land_use_type_id, exempt_codes)


## Export back to database.


@sim.model()
def export_mar(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df.res_type[df.res_type.isnull()] = ''
    df_to_db(df, 'attributes_mar', schema=staging)

sim.run(['export_mar'])
