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


tf = TableFrame(staging.parcels_nap, index_col='asmt')
sim.add_table('parcels_in', tf, copy_col=False)


@sim.table(cache=True)
def buildings():
    df = loader.get_attributes('built/parcel/2010/nap/Napa_buildings.dbf')

    # Usually duplicate records are similar, but sometimes the last record
    # appears to have more information.
    df.drop_duplicates('FeeParcel', take_last=True, inplace=True)

    df.set_index('FeeParcel', inplace=True)
    assert df.index.is_unique
    assert not df.index.hasnans()
    return df


@sim.table(cache=True)
def taxroll():
    df = loader.get_attributes('built/parcel/2010/nap/Napa_taxroll.dbf')

    # Take the last of duplicate records for consistency with buildings
    # table, but this might not be the right approach.
    df.drop_duplicates('Asmt', take_last=True, inplace=True)

    df.set_index('Asmt', inplace=True)
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
    return '055'


@out
def parcel_id_local():
    pass


@out
def land_use_type_id(code='parcels_in.landuse1'):
    # Take last land use code when deduplicating.
    return code.groupby(level=0).last()


@out
def land_value(value='taxroll.CurrentMar'):
    # Alternate inputs:
    # - "CurrentNet"
    # Need to separate land value and improvement value.
    # Assumed crude 50-50 split.
    return 0.5 * value


@out
def improvement_value(value='taxroll.CurrentMar'):
    # Alternate inputs:
    # - "CurrentNet"
    # Need to separate land value and improvement value.
    # Assumed crude 50-50 split.
    return 0.5 * value


@out
def year_assessed(year='taxroll.TaxYear'):
    # It is not clear what year "CurrentMar" refers to, but assume it
    # refers to "TaxYear".
    year.replace(0, np.nan, inplace=True)
    return year


@out
def year_built(year='buildings.YearBuilt'):
    year.replace(0, np.nan, inplace=True)
    return year


@out
def building_sqft(sqft='buildings.BuildingSi'):
    return sqft


@out
def non_residential_sqft(sqft='buildings.SqFtOffice'):
    return sqft


# @out
# def residential_units(units='buildings.NumUnitsRe'):
    # return units
# @out
# def residential_units(tot_units = 'buildings.NumUnitsRe', res_type = 'parcels_in.landuse1'):

    # units = pd.Series(index=res_type.index)
    # tot_units = tot_units.reindex(units.index, copy=False)
    # units[tot_units > 0] = tot_units
    
    ## If single family residential and no current units, assume one residential unit.
    # units[res_type.isin(['11','111'])*np.logical_or((tot_units==0), tot_units.isnull())] = 1
    
    # return units
    
@out
def residential_units(tot_units = 'buildings.NumUnitsRe', res_type = 'parcels_in.landuse1'):

    units = pd.Series(index=tot_units.index)
    res_type = res_type.groupby(level=0).last()
    res_type = res_type.reindex(tot_units.index, copy = False)
    units[tot_units > 0] = tot_units
    
    # If single family residential and no current units, assume one residential unit.
    units[res_type.isin(['11','111','05','23','31','32','39','312','313','314','315','322','323','324','325','392','393','394','395','3101','3201','3901','12'])*np.logical_or((tot_units==0), tot_units.isnull())] = 1
    
    # If land use implies 2 units and no current units, assume 2 residential units.
    units[res_type.isin(['212', '3921', '2122'])*np.logical_or((tot_units==0), tot_units.isnull())] = 2
    
    # If land use implies 3 units and no current units, assume 3 residential units.
    units[res_type.isin(['21','213','3931','2133'])*np.logical_or((tot_units==0), tot_units.isnull())] = 3
    
    # If land use implies 4 units and no current units, assume 4 residential units.
    units[res_type.isin(['214', '3941', '2144'])*np.logical_or((tot_units==0), tot_units.isnull())] = 4
    
    # If land use implies 7 units and no current units, assume 7 residential units.
    units[res_type.isin(['215'])*np.logical_or((tot_units==0), tot_units.isnull())] = 7
    
    # If land use implies 14 units and no current units, assume 14 residential units.
    units[res_type.isin(['216'])*np.logical_or((tot_units==0), tot_units.isnull())] = 14
    
    # If land use implies 30 units and no current units, assume 30 residential units.
    units[res_type.isin(['217'])*np.logical_or((tot_units==0), tot_units.isnull())] = 30

    # If land use implies 50 units and no current units, assume 50 residential units.
    units[res_type.isin(['218'])*np.logical_or((tot_units==0), tot_units.isnull())] = 50
    
    return units
    
@out
def res_type(tot_units = 'buildings.NumUnitsRe', res_type = 'parcels_in.landuse1'):

    units = pd.Series(index=tot_units.index)
    res_type = res_type.groupby(level=0).last()
    res_type = res_type.reindex(tot_units.index, copy = False)
    
    # If single family residential
    res_type[res_type.isin(['11','111','05','23','31','32','39','312','313','314','315','322','323','324','325','392','393','394','395','3101','3201','3901','12'])] = 'single'
    
    # If multifamily
    res_type[res_type.isin(['212', '3921', '2122','21','213','3931','2133','214', '3941', '2144','215','216','217','218'])] = 'multi'

    res_type[~res_type.isin(['single','multi'])] = 'other'
    
    return res_type
    
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
def stories(stories='parcels_in.floor'):
    # Take greatest number of stories when deduplicating.
    return stories.groupby(level=0).max().astype(float)


@out
def tax_exempt(land_use_type_id='parcels_out.land_use_type_id'):
    return utils.get_tax_exempt(land_use_type_id, exempt_codes)


## Export back to database.


@sim.model()
def export_nap(parcels_out):
    df = parcels_out.to_frame()
    assert df.index.is_unique
    assert not df.index.hasnans()
    df_to_db(df, 'attributes_nap', schema=staging)

sim.run(['export_nap'])
