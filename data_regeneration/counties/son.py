import string

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
# Residential status was classified based on "LandUseDesc" column.
res_codes = {'single': ['0010', '0051'],
             'multi': [string.zfill(num, 4) for num in
                       [0, 1, 5, 11, 12] + range(15, 51) + range(52, 58)],
             'mixed': ['0013', '0014', '0177', '0201', '0392']}
exempt_codes = []


## Register input tables.


tf = TableFrame(staging.parcels_son, index_col='apn')

sim.add_table('parcels_in', tf, copy_col=False)


@sim.table(cache=True)
def abag():
    def format_apn(num):
        s = string.zfill(num, 9)
        assert len(s) <= 9
        return "{}-{}-{}".format(s[:3], s[3:6], s[6:])

    csv = loader.get_path('built/parcel/2010/son/ABAG_SonomaCounty.txt')
    df = pd.read_csv(csv, low_memory=False, parse_dates=[17])

    # Rename ".Units" to "Units".
    df.rename(columns={'.Units': 'Units'}, inplace=True)

    # Create apn index based on FeeParcel column.
    df['apn'] = df.FeeParcel.floordiv(1000).apply(format_apn)
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
    return '097'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(code='abag.LandUse'):
    return code


@out
def res_type(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_res_type(land_use_type_id, res_codes)


@out
def land_value(value='abag.LandValue'):
    return value


@out
def improvement_value(value='abag.StructureValue'):
    return value


@out
def year_assessed(date='abag.LastSaleDate'):
    year = date.apply(lambda d: d.year if d else np.nan)
    year.replace([-1, 0], np.nan, inplace=True)
    return year


@out
def year_built(year='abag.YearBuilt'):
    year.replace(0, np.nan, inplace=True)
    return year


@out
def building_sqft(sqft='abag.BuildingSizePRIMARY'):
    return sqft


@out
def non_residential_sqft(building_sqft='parcels_out.building_sqft',
                         res_type='parcels_out.res_type',
                         residential_units='parcels_out.residential_units'):
    return utils.get_nonresidential_sqft(building_sqft, res_type,
                                         residential_units)


@out
def residential_units(tot_units='abag.Units',
                      res_type='parcels_out.res_type'):
    return utils.get_residential_units(tot_units, res_type)

@out
def condo_identifier(merged_situs = 'abag.MergedSitus', year_built = 'abag.YearBuilt', city_code = 'abag.SitusCityCode', lu_code = 'abag.LandUse'):
    
    merged_situs[merged_situs.str.len() < 8] = '        '
    merged_situs[merged_situs.isnull()] = '        '
    merged_situs = merged_situs.str.slice(0,8)
    lu_code[lu_code.str.len() != 4] = '9999'
    lu_code[lu_code.isnull()] = '9999'
    year_built[year_built.isnull()] = 9999
    year_built[year_built == 0] = 8888
    year_built = year_built.astype('str')
    city_code[city_code.isnull()] = '  '
    code = merged_situs + lu_code + year_built + city_code
    code[code.isnull()] = ''
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
def export_son(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_son', schema=staging)

sim.run(['export_son'])
