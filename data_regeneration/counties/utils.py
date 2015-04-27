import numpy as np
import pandas as pd


# Assume that each residential unit in a mixed-used parcel occupies
# 1500 sqft, since residential vs. non-residential sqft is not known.
sqft_per_res_unit = 1500.


def get_res_type(land_use_type_id, res_codes):
    lu = pd.Series(index=land_use_type_id.index, dtype=object)
    for name, codes in res_codes.items():
        lu[land_use_type_id.isin(codes)] = name
    return lu


def get_nonresidential_sqft(building_sqft, res_type, residential_units):
    sqft = pd.Series(index=res_type.index)
    building_sqft = building_sqft.reindex(sqft.index, copy=False)

    # If not residential, assume all area is non-residential.
    sqft[res_type.isnull()] = building_sqft

    # If residential, assume zero non-residential area.
    sqft[(res_type == 'single') | (res_type == 'multi')] = 0

    # If mixed-use, assume residential units occupy some area.
    sqft[res_type == 'mixed'] = (building_sqft -
                                 sqft_per_res_unit * residential_units)

    # Non-residential area must not be negative.
    sqft[(sqft.notnull()) & (sqft < 0)] = 0

    return sqft


def get_residential_units(tot_units, res_type):
    units = pd.Series(index=res_type.index)
    tot_units = tot_units.reindex(units.index, copy=False)

    # If not residential, assume zero residential units.
    units[res_type.isnull()] = 0

    # If single family residential, assume one residential unit.
    units[res_type == 'single'] = 1

    # If non-single residential, assume all units are all residential,
    # even if mixed-use.
    units[res_type == 'multi'] = tot_units
    units[res_type == 'mixed'] = tot_units
    
    # If multi-family and zero units, assume one residential unit for now (leave further imputation to the imputation section).
    units[(res_type == 'multi')& np.logical_or((units == 0), (units.isnull()))] = 1

    return units


def get_sqft_per_unit(building_sqft, non_residential_sqft, residential_units):
    per_unit = (1. * building_sqft - non_residential_sqft) / residential_units
    per_unit.replace(np.inf, np.nan, inplace=True)
    return per_unit


def get_tax_exempt(land_use_type_id, exempt_codes):
    exempt = pd.Series(index=land_use_type_id.index, dtype=int)
    exempt[land_use_type_id.isin(exempt_codes)] = 1
    exempt[~land_use_type_id.isin(exempt_codes)] = 0
    return exempt
