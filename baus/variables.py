import numpy as np
import pandas as pd
from urbansim.utils import misc
import orca
from baus import datasources
from baus.utils import nearest_neighbor, groupby_random_choice
from urbansim_defaults import utils
from urbansim_defaults import variables


'''
There's some really interesting side-effects of orca cache behavior which
affects how the cache is used here - see this for more info
https://github.com/UDST/orca/issues/16
In general I try to be very conservative on when to use the cache
I don't trust myself to be able to use it right
'''

#####################
# HOUSEHOLDS VARIABLES
#####################


@orca.column('households', cache=True)
def tmnode_id(households, buildings):
    return misc.reindex(buildings.tmnode_id, households.building_id)


#####################
# COSTAR VARIABLES
#####################


@orca.column('costar')
def juris_ave_income(parcels, costar):
    return misc.reindex(parcels.juris_ave_income, costar.parcel_id)


@orca.column('costar')
def is_sanfran(parcels, costar):
    return misc.reindex(parcels.is_sanfran, costar.parcel_id)


@orca.column('costar')
def general_type(costar):
    return costar.PropertyType


@orca.column('costar')
def node_id(parcels, costar):
    return misc.reindex(parcels.node_id, costar.parcel_id)


@orca.column('costar')
def tmnode_id(parcels, costar):
    return misc.reindex(parcels.tmnode_id, costar.parcel_id)


@orca.column('costar')
def zone_id(parcels, costar):
    return misc.reindex(parcels.zone_id, costar.parcel_id)


@orca.column('costar')
def transit_type(costar, parcels_geography):
    return misc.reindex(parcels_geography.tpp_id, costar.parcel_id).\
        reindex(costar.index).fillna('none')


#####################
# JOBS VARIABLES
#####################


@orca.column('jobs', cache=True)
def tmnode_id(jobs, buildings):
    return misc.reindex(buildings.tmnode_id, jobs.building_id)


@orca.column('jobs', cache=True)
def naics(jobs):
    return jobs.sector_id


@orca.column('jobs', cache=True)
def empsix_id(jobs, mapping):
    return jobs.empsix.map(mapping['empsix_name_to_id'])


#############################
# RESIDENTIAL UNITS VARIABLES
#############################

# move zone_id from buildings to residential units
@orca.column('residential_units', cache=True)
def zone_id(residential_units, buildings):
    return misc.reindex(buildings.zone_id, residential_units.building_id)


@orca.column('residential_units')
def submarket_id(residential_units, buildings):
    # The submarket is used for supply/demand equilibration. It's the same
    # as the zone_id, but in a separate column to avoid name conflicts when
    # tables are merged.
    return misc.reindex(buildings.zone_id, residential_units.building_id)


@orca.column('residential_units')
def vacant_units(residential_units, households):
    return residential_units.num_units.sub(
        households.unit_id[
            households.unit_id != -1].value_counts(), fill_value=0)


#####################
# BUILDINGS VARIABLES
#####################


@orca.column('buildings', cache=True)
def general_type(buildings, building_type_map):
    return buildings.building_type.map(building_type_map)


# I want to round this cause otherwise we'll be underfilling job spaces
# in the aggregate because of rounding errors - this way some spaces will
# be underfilled and others overfilled which should yield an average of
# the sqft_per_job table
@orca.column('buildings', cache=True)
def job_spaces(buildings):
    return (buildings.non_residential_sqft /
            buildings.sqft_per_job).fillna(0).round().astype('int')


@orca.column('buildings')
def vacant_job_spaces(buildings, jobs):
    s = jobs.building_id[jobs.building_id != -1]
    return buildings.job_spaces.sub(
        s.value_counts(), fill_value=0).astype('int')


@orca.column('buildings')
def vacant_res_units(buildings, households):
    s = households.building_id[households.building_id != -1]
    return buildings.residential_units.sub(
        s.value_counts(), fill_value=0).astype('int')


@orca.column('buildings', cache=True)
def sqft_per_job(buildings, building_sqft_per_job, sqft_per_job_adjusters, telecommute_sqft_per_job_adjusters, taz_geography, base_year, year, run_setup):
    
    # data hack here, setting missing building type data to OF - should be handled upstream
    sqft_per_job = buildings.building_type.fillna("O").map(building_sqft_per_job)

    superdistrict = misc.reindex(taz_geography.superdistrict, buildings.zone_id)

    # this factor changes all sqft per job according to which superdistrict the building is in - this is so denser areas can have lower sqft per job
    # this is a simple multiply so a number 1.1 increases the sqft per job by 10% and .9 decreases it by 10%

    # if adjusters are enabled, adjust sqft_per_job rates for all years
    if run_setup["run_telecommute_strategy"] and year != base_year:
        sqft_per_job = sqft_per_job * superdistrict.map(telecommute_sqft_per_job_adjusters['sqft_per_job_factor_{}'.format(year)])
	# if the telecommute strategy is enabled, instead adjust future year sqft_per_job rates with the factor for that year
    elif run_setup["sqft_per_job_adjusters"]:
        sqft_per_job = sqft_per_job * superdistrict.map(sqft_per_job_adjusters['sqft_per_job_factor'])

    return sqft_per_job


@orca.column('buildings', cache=True)
def building_age(buildings, year):
    return (year or 2014) - buildings.year_built


@orca.column('buildings')
def price_per_sqft(buildings):
    s = buildings.redfin_sale_price / buildings.sqft_per_unit
    # not a huge fan of this.  it's an error to have nas in rsh_estimate even
    # though nas will be filtered out by the hedonic - error should happen
    # after filters are applied
    return s.reindex(buildings.index).fillna(-1)


@orca.column('buildings', cache=True)
def transit_type(buildings, parcels_geography):
    return misc.reindex(parcels_geography.tpp_id, buildings.parcel_id).\
        reindex(buildings.index).fillna('none')


@orca.column('buildings')
def unit_price(buildings):
    return buildings.residential_price * buildings.sqft_per_unit


@orca.column('buildings', cache=True)
def tmnode_id(buildings, parcels):
    return misc.reindex(parcels.tmnode_id, buildings.parcel_id)


@orca.column('buildings')
def juris_ave_income(parcels, buildings):
    return misc.reindex(parcels.juris_ave_income, buildings.parcel_id)


@orca.column('buildings', cache=True)
def is_sanfran(parcels, buildings):
    return misc.reindex(parcels.is_sanfran, buildings.parcel_id)


@orca.column('buildings', cache=True)
def sqft_per_unit(buildings):
    return (buildings.building_sqft /
            buildings.residential_units.replace(0, 1)).clip(400, 6000)


@orca.column('buildings', cache=True)
def modern_condo(buildings):
    # this is to try and differentiate between new construction
    # in the city vs in the burbs
    return ((buildings.year_built > 2000) * (buildings.building_type == "HM"))\
        .astype('int')


@orca.column('buildings', cache=True)
def new_construction(buildings):
    return (buildings.year_built > 2000).astype('int')


@orca.column('buildings', cache=True)
def historic(buildings):
    return (buildings.year_built < 1940).astype('int')


@orca.column('buildings', cache=True)
def vmt_res_cat(buildings, run_setup):
    if run_setup['run_vmt_fee_res_for_res_strategy'] or run_setup["run_sb_743_strategy"]:
        vmt_fee_categories = orca.get_table("vmt_fee_categories")
        return misc.reindex(vmt_fee_categories.res_cat, buildings.zone_id)


@orca.column('buildings', cache=True)
def vmt_nonres_cat(buildings, run_setup):
    if (run_setup['run_vmt_fee_com_for_com_strategy'] or run_setup['run_vmt_fee_com_for_res_strategy']):
        vmt_fee_categories = orca.get_table("vmt_fee_categories")
        return misc.reindex(vmt_fee_categories.nonres_cat, buildings.zone_id)


@orca.column('buildings', cache=True)
def residential_price(buildings, residential_units, developer_settings):
    """
    This was originally an orca.step in the ual code.  This allows model steps
    like 'price_vars' and 'feasibility' to read directly from the buildings
    table, by aggregating up prices/rents from the units table.

    We currently set the building price per square foot to be the higher of
    the average (a) unit price per square foot or (b) unit price-adjusted
    rent per square foot.

    Data expectations
    -----------------
    - 'residential_units' table has following columns:
        - 'unit_residential_price' (float, 0-filled)
        - 'unit_residential_rent' (float, 0-filled)
        - 'building_id' (int, non-missing, corresponds to index of
          'buildings' table)
    - 'buildings' table has following columns:
        - index that serves as its id
        - 'residential_price' (float, 0-filled)
    - 'settings' injectable has a 'cap_rate' (float, range 0 to 1)
    """

    # Verify initial data characteristics
    '''
    ot.assert_orca_spec(OrcaSpec(
        '',
        TableSpec(
            'residential_units',
            ColumnSpec('unit_residential_price', min=0),
            ColumnSpec('unit_residential_rent', min=0),
            ColumnSpec(
                'building_id', foreign_key='buildings.building_id',
                missing=False)),
        TableSpec(
            'buildings',
            ColumnSpec('building_id', primary_key=True),
            ColumnSpec('residential_price', min=0)),
        InjectableSpec('settings', min=0, max=1)))
    '''

    cols = ['building_id', 'unit_residential_price', 'unit_residential_rent']
    means = residential_units.to_frame(cols).groupby(['building_id']).mean()

    # Convert monthly rent to equivalent sale price
    cap_rate = developer_settings.get('cap_rate')
    means['unit_residential_rent'] = \
        means.unit_residential_rent * 12 / cap_rate

    # Calculate max of price or rent, by building
    return means.max(axis=1)


@orca.column('buildings', cache=True, cache_scope='iteration')
def cml(buildings, parcels):
    return misc.reindex(parcels.cml, buildings.parcel_id)


@orca.column('buildings', cache=True, cache_scope='iteration')
def cnml(buildings, parcels):
    return misc.reindex(parcels.cnml, buildings.parcel_id)


@orca.column('buildings', cache=True, cache_scope='iteration')
def combo_logsum(buildings, parcels):
    return misc.reindex(parcels.combo_logsum, buildings.parcel_id)


#####################
# NODES VARIABLES
#####################


# these are computed outcomes of accessibility variables
@orca.column('nodes')
def retail_ratio(nodes):
    # then compute the ratio of income to retail sqft - a high number here
    # indicates an underserved market
    return nodes.sum_income_3000 / nodes.retail_sqft_3000.clip(lower=1)


#####################
# PARCELS VARIABLES
#####################

@orca.column('parcels')
def maz_id(parcels, parcel_to_maz):
    return parcel_to_maz.maz.reindex(parcels.index)


@orca.column("parcels")
def residential_sales_price_sqft(parcel_sales_price_sqft_func):
    return parcel_sales_price_sqft_func("residential")


@orca.column('parcels')
def sdem(development_projects, parcels):
    return parcels.geom_id.isin(development_projects.geom_id).astype('int')


@orca.column('parcels')
def retail_ratio(parcels, nodes):
    return misc.reindex(nodes.retail_ratio, parcels.node_id)


# the stories attributes on parcels will be the max story
# attribute on the buildings
@orca.column('parcels', cache=True)
def stories(buildings):
    return buildings.stories.groupby(buildings.parcel_id).max()


@orca.column('parcels', cache=True)
def height(parcels):
    return parcels.stories * 12  # hard coded 12 feet per story


@orca.column('parcels', cache=True)
def vmt_res_cat(parcels, run_setup):
    if (run_setup['run_vmt_fee_com_for_com_strategy'] or run_setup['run_vmt_fee_com_for_res_strategy'] or run_setup["run_sb_743_strategy"]):
        vmt_fee_categories = orca.get_table("vmt_fee_categories")
        return misc.reindex(vmt_fee_categories.res_cat, parcels.zone_id)


@orca.column('parcels', cache=True)
def vmt_nonres_cat(parcels, run_setup):
    if run_setup['run_vmt_fee_res_for_res_strategy']:
        vmt_fee_categories = orca.get_table("vmt_fee_categories")
        return misc.reindex(vmt_fee_categories.nonres_cat, parcels.zone_id)


# residential fees
@orca.column('parcels', cache=True)
def vmt_res_fees(parcels, run_setup):
    if run_setup['run_vmt_fee_res_for_res_strategy']:
        account_strategies = orca.get_table("account_strategies")
        vmt_settings = account_strategies["acct_settings"]["vmt_settings"]
        res_fees = parcels.vmt_res_cat.map(vmt_settings["res_for_res_fee_amounts"]) if run_setup["run_vmt_fee_res_for_res_strategy"] else 0
        return res_fees


# commercial fees
@orca.column('parcels', cache=True)
def vmt_com_fees(parcels, run_setup):
    if (run_setup['run_vmt_fee_com_for_com_strategy'] or run_setup['run_vmt_fee_com_for_res_strategy']):
        account_strategies = orca.get_table("account_strategies")
        vmt_settings = account_strategies["acct_settings"]["vmt_settings"]
        com_for_res_fees = parcels.vmt_nonres_cat.map(vmt_settings["com_for_res_fee_amounts"]) if \
            run_setup["run_vmt_fee_com_for_res_strategy"] else 0
        com_for_com_fees = parcels.vmt_nonres_cat.map(vmt_settings["com_for_com_fee_amounts"]) if \
            run_setup ["run_vmt_fee_com_for_com_strategy"] else 0
        com_fees = com_for_res_fees + com_for_com_fees
        return com_fees


# compute the fees per unit for each parcel
# (since feees are specified spatially)
@orca.column('parcels', cache=True)
def fees_per_unit(parcels, run_setup):
    if run_setup["run_vmt_fee_res_for_res_strategy"]:
        s = pd.Series(0, index=parcels.index)
        s += parcels.vmt_res_fees
        return s


# since this is by sqft this implies commercial
@orca.column('parcels', cache=True)
def fees_per_sqft(parcels, run_setup):
    if run_setup["run_vmt_fee_com_for_com_strategy"] or run_setup["run_vmt_fee_com_for_res_strategy"]:
        s = pd.Series(0, index=parcels.index)
        s += parcels.vmt_com_fees
        return s


@orca.column('parcels', cache=True)
def pda_id(parcels, parcels_geography):
    return parcels_geography.pda_id.reindex(parcels.index)


@orca.column('parcels', cache=True)
def cat_id(parcels, parcels_geography):
    return parcels_geography.cat_id.reindex(parcels.index)


@orca.column('parcels', cache=True)
def tra_id(parcels, parcels_geography):
    return parcels_geography.tra_id.reindex(parcels.index)


@orca.column('parcels', cache=True)
def juris_tra(parcels, parcels_geography):
    return parcels_geography.juris_tra.reindex(parcels.index)


@orca.column('parcels', cache=True)
def sesit_id(parcels, parcels_geography):
    return parcels_geography.sesit_id.reindex(parcels.index)


@orca.column('parcels', cache=True)
def juris_sesit(parcels, parcels_geography):
    return parcels_geography.juris_sesit.reindex(parcels.index)


@orca.column('parcels', cache=True)
def ppa_id(parcels, parcels_geography):
    return parcels_geography.ppa_id.reindex(parcels.index)


@orca.column('parcels', cache=True)
def juris_ppa(parcels, parcels_geography):
    return parcels_geography.juris_ppa.reindex(parcels.index)


@orca.column('parcels', cache=True)
def coc_id(parcels, parcels_geography):
    return parcels_geography.coc_id.reindex(parcels.index)


@orca.column('parcels', cache=True)
def juris_coc(parcels, parcels_geography):
    return parcels_geography.juris_coc.reindex(parcels.index)


@orca.column('parcels', cache=True)
def superdistrict(parcels, taz_geography):
    return misc.reindex(taz_geography.superdistrict, parcels.zone_id)


@orca.column('parcels', cache=True)
def subregion(parcels, taz_geography):
    return misc.reindex(taz_geography.subregion, parcels.zone_id)


# perffoot is a dummy indicating the FOOTprint for the PERFormance targets
@orca.column('parcels', cache=True)
def urban_footprint(parcels, parcels_geography):
    return parcels_geography.perffoot.reindex(parcels.index)


# perfzone is a dummy for geography for a performance target
@orca.column('parcels', cache=True)
def performance_zone(parcels, parcels_geography):
    return parcels_geography.perfarea.reindex(parcels.index)


# urbanized is a dummy for urbanized area, sourced from shapefile at:
# M:\urban_modeling\data\LandUse\landuse_raw\urban_footprint_2009
@orca.column('parcels', cache=True)
def urbanized(parcels, parcels_geography):
    return parcels_geography.urbanized.reindex(parcels.index)


@orca.column('parcels', cache=True)
def juris(parcels, parcels_geography):
    return parcels_geography.juris_name


@orca.column('parcels', cache=True)
def ave_sqft_per_unit(parcels, zones, data_edits):
    s = misc.reindex(zones.ave_unit_sqft, parcels.zone_id)

    clip = data_edits.get("ave_sqft_per_unit_clip", None)
    if clip is not None:
        s = s.clip(lower=clip['lower'], upper=clip['upper'])

    '''
    This is a fun feature that lets you set max dua for new contruction based
    on the dua (as an indicator of density and what part of the city we are).
    Example use in the YAML:

    clip_sqft_per_unit_based_on_dua:
      - threshold: 50
        max: 1000
      - threshold: 100
        max: 900
      - threshold: 150
        max: 800
    '''
    cfg = data_edits.get("clip_sqft_per_unit_based_on_dua", None)
    if cfg is not None:
        for clip in cfg:
            s[parcels.max_dua >= clip["threshold"]] = clip["max"]

    return s


# these are actually functions that take parameters,
# but are parcel-related so are defined here
@orca.injectable("parcel_sales_price_sqft_func", autocall=False)
def parcel_average_price(use, quantile=.5):
    if use == "residential":
        # get node price average and put it on parcels
        s = misc.reindex(orca.get_table('nodes')[use], orca.get_table('parcels').node_id)

        if orca.get_injectable("run_setup")["cost_shifters"]:
            cost_shifters = orca.get_table("parcels").cost_shifters
            s = s / cost_shifters

        # just to make sure we're in a reasonable range
        return s.fillna(0).clip(150, 1250)

    if 'nodes' not in orca.list_tables():
        # just to keep from erroring
        return pd.Series(0, orca.get_table('parcels').index)

    return misc.reindex(orca.get_table('nodes')[use], orca.get_table('parcels').node_id)


#############################
# Functions for Checking
# Allowed Uses and Building
# Types on Parcels
#############################


@orca.injectable("parcel_is_allowed_func", autocall=False)
def parcel_is_allowed(form):
    zoning_adjusters = orca.get_injectable("zoning_adjusters")
    mapping = orca.get_injectable("mapping")
    form_to_btype = mapping["form_to_btype"]

    # we have zoning by building type but want
    # to know if specific forms are allowed
    zoning_existing = orca.get_table("zoning_existing")
    zoning_strategy = orca.get_table("zoning_strategy")
    parcels = orca.get_table("parcels")

    allowed = pd.Series(0, index=parcels.index)

    # first, it's allowed if any building type that matches
    # the form is allowed
    for typ in form_to_btype[form]:
        allowed |= zoning_existing[typ]

    # then we override it with any values that are specified in the strategies
    # i.e. they come from the add_bldg and drop_bldg columns
    for typ in form_to_btype[form]:
        allowed = zoning_strategy[typ].combine_first(allowed)

    # notice there is some dependence on ordering here.  basically values take
    # precedent that occur LAST in the form_to_btype mapping

    # this is a fun modification - when we get too much retail in jurisdictions
    # we can just eliminate all retail
    if "eliminate_retail_zoning_from_juris" in zoning_adjusters and form == "retail":
        allowed *= ~orca.get_table("parcels").juris.isin(
            zoning_adjusters["eliminate_retail_zoning_from_juris"])

    return allowed.astype("bool")


@orca.column('parcels')
def first_building_type(buildings, parcels):
    df = buildings.to_frame(columns=['building_type', 'parcel_id'])
    return df.groupby('parcel_id').building_type.first()


@orca.injectable(autocall=False)
def parcel_first_building_type_is(form):
    form_to_btype = orca.get_injectable('mapping')["form_to_btype"]
    parcels = orca.get_table('parcels')
    return parcels.first_building_type.isin(form_to_btype[form])


@orca.column('parcels')
def juris_ave_income(households, buildings, parcels_geography, parcels):
    # get frame of income and jurisdiction
    h = orca.merge_tables("households",
                          [households, buildings, parcels_geography],
                          columns=["jurisdiction_id", "income"])
    # get median income by jurisdiction
    s = h.groupby(h.jurisdiction_id).income.quantile(.5)
    # adding a fix here for negative incomes in the households table
    # so that the later steps don't crash
    s.loc[s < 1] = 1
    # map it to parcels - fill na with median for all areas
    # should probably remove the log transform and do that in the models
    return misc.reindex(s, parcels_geography.jurisdiction_id).\
        reindex(parcels.index).fillna(s.median()).apply(np.log1p)


# returns the newest building on the land and fills
# missing values with 1800 - for use with development limits
@orca.column('parcels')
def newest_building(parcels, buildings):
    return buildings.year_built.groupby(buildings.parcel_id).max().\
        reindex(parcels.index).fillna(1800)


# this returns the set of parcels which have been marked as
# disapproved by "the button" - only equals true when disallowed
@orca.column('parcels', cache=True)
def manual_nodev(parcel_rejections):
    df = parcel_rejections.to_frame()
    df = df[df.state == "denied"]
    s = df.parcelId
    return s.astype('int')


@orca.column('parcels')
def oldest_building_age(parcels, year):
    return year - parcels.oldest_building.replace(9999, 0)


@orca.column('parcels', cache=True)
def is_sanfran(parcels_geography, buildings, parcels):
    return (parcels_geography.juris_name == "San Francisco").\
        reindex(parcels.index).fillna(False).astype('int')


@orca.column('parcels', cache=True)
def total_non_residential_sqft(parcels, buildings):
    return buildings.non_residential_sqft.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)


@orca.column('parcels')
def nodev(zoning_existing, parcels, static_parcels):
    # nodev from zoning
    s1 = zoning_existing.nodev.reindex(parcels.index).\
        fillna(0).astype('bool')
    # nodev from static parcels - this marks nodev those parcels which are
    # marked as "static" - any parcels which should not be considered by the
    # developer model may be marked as static
    s2 = parcels.index.isin(static_parcels)
    # nodev from sea level rise- determined by hazards.py model
    if 'slr_nodev' in parcels.columns:
        s3 = np.array(parcels['slr_nodev'])
        return s1 | s2 | s3
    else:
        return s1 | s2


# get built far but set to nan for small parcels
@orca.column('parcels')
def built_far(parcels):
    # compute the actually built farn on a parcel
    s = parcels.total_sqft / parcels.parcel_size
    # if the parcel size is too small to get an accurate reading, remove it
    s[parcels.parcel_acres < .1] = np.nan
    return s


# actual columns start here
@orca.column('parcels')
def max_far(parcels_zoning_calculations, parcels, zoning_adjusters):
    # first we combine the zoning columns
    s = parcels_zoning_calculations.effective_max_far * ~parcels.nodev

    # we had trouble with the zoning outside of the footprint
    # make sure we have rural zoning outside of the footprint
    s2 = parcels.urban_footprint.map({0: 0, 1: np.nan})
    s = pd.concat([s, s2], axis=1).min(axis=1)

    if zoning_adjusters["dont_build_most_dense_building"]:
        # in this case we shrink the zoning such that we don't built the
        # tallest building in a given zone
        # if there no building in the zone currently, we make the max_far = .2
        s2 = parcels.built_far.groupby(parcels.zone_id).max()
        s2 = misc.reindex(s2, parcels.zone_id).fillna(.2)
        s = pd.concat([s, s2], axis=1).min(axis=1)

    return s


# compute built dua and set to nan if small parcels
@orca.column('parcels')
def built_dua(parcels):
    # compute the actually built dua on a parcel
    s = parcels.total_residential_units / parcels.parcel_acres
    # if the parcel size is too small to get an accurate reading, remove it
    s[parcels.parcel_acres < .1] = np.nan
    return s


@orca.column('parcels')
def max_dua(parcels_zoning_calculations, parcels, zoning_adjusters):
    # first we combine the zoning columns
    s = parcels_zoning_calculations.effective_max_dua * ~parcels.nodev

    # we had trouble with the zoning outside of the footprint
    # make sure we have rural zoning outside of the footprint
    s2 = parcels.urban_footprint.map({0: .01, 1: np.nan})
    s = pd.concat([s, s2], axis=1).min(axis=1)

    if zoning_adjusters["dont_build_most_dense_building"]:
        # in this case we shrink the zoning such that we don't built the
        # tallest building in a given zone
        # if there no building in the zone currently, we make the max_dua = 4
        s2 = parcels.built_dua.groupby(parcels.zone_id).max()
        s2 = misc.reindex(s2, parcels.zone_id).fillna(4)
        s = pd.concat([s, s2], axis=1).min(axis=1)

    return s


@orca.column('parcels')
def general_type(parcels, buildings):
    s = buildings.general_type.groupby(buildings.parcel_id).first()
    return s.reindex(parcels.index).fillna("Vacant")


# for debugging reasons this is split out into its own function
@orca.column('parcels')
def building_purchase_price_sqft(parcels, developer_settings):
    price = pd.Series(0, parcels.index)
    gentype = parcels.general_type
    cap_rate = developer_settings["cap_rate"]
    # all of these factors are above one which does some discouraging of
    # redevelopment - this will need to be recalibrated when the new
    # developer model comes into play
    for form in ["Office", "Retail", "Industrial", "Residential"]:
        # convert to price per sqft from yearly rent per sqft
        factor = 1.4 if form == "Residential" else (1/cap_rate)
        # raise cost to convert from industrial
        if form == "Industrial":
            factor *= 3.0
        if form == "Retail":
            factor *= 2.0
        if form == "Office":
            factor *= 1.4
        tmp = parcel_average_price(form.lower())
        price += tmp * (gentype == form) * factor

    # this is not a very constraining clip
    return price.clip(150, 2500)


@orca.column('parcels')
def building_purchase_price(parcels):
    # the .8 is because we assume the building only charges for 80% of the
    # space - when a good portion of the building is parking, this probably
    # overestimates the part of the building that you can charge for.
    # the second .85 is because the price is likely to be lower for existing
    # buildings than for one that is newly built
    return (parcels.total_sqft * parcels.building_purchase_price_sqft *
            .8 * .85).reindex(parcels.index).fillna(0)


@orca.column('parcels')
def land_cost(parcels):
    # would be much nicer to have a model on land price, but we make an
    # assumption that the price is a ratio the prevailing rate for floor
    # area.  this does not take into account the zoning, which would mean
    # land which can be developer more fully would be more valuable.
    # generally this is not as strong an indicator of valueable land as
    # high prices (see dense zoning in downtown oakland)
    s = (parcels.building_purchase_price_sqft / 40).clip(5, 20)
    # industrial is an exception as cleanup is likely to be done - would
    # be nice to have data on superfund sites and such
    s[parcels.general_type == "Industrial"] = 100
    return parcels.building_purchase_price + parcels.parcel_size * s


@orca.column('parcels', cache=True)
def county(parcels, mapping):
    return parcels.county_id.map(mapping["county_id_map"])


@orca.column('parcels', cache=True)
def cost_shifters(parcels, cost_shifters):
    return parcels.county.map(cost_shifters["cost_shifters"])


@orca.column('parcels', cache=True)
def taz2(parcels, maz):
    return misc.reindex(maz.TAZ, parcels.maz_id)


@orca.column('parcels', cache=True)
def node_id(parcels, net):
    s = net["walk"].get_node_ids(parcels.x, parcels.y)
    fill_val = s.value_counts().index[0]
    s = s.reindex(parcels.index).fillna(fill_val).astype('int')
    return s


@orca.column('parcels', cache=True)
def tmnode_id(parcels, net):
    s = net["drive"].get_node_ids(parcels.x, parcels.y)
    fill_val = s.value_counts().index[0]
    s = s.reindex(parcels.index).fillna(fill_val).astype('int')
    return s


@orca.column('parcels', cache=True)
def subregion(taz_geography, parcels):
    return misc.reindex(taz_geography.subregion, parcels.zone_id)


@orca.column('parcels', cache=True)
def vmt_code(parcels, run_setup):
    if run_setup['run_vmt_fee_res_for_res_strategy']:
        vmt_fee_categories = orca.get_table("vmt_fee_categories")
        return misc.reindex(vmt_fee_categories.res_cat, parcels.zone_id)


@orca.column('parcels', cache=True)
def subzone(parcels, parcels_subzone):
    return parcels_subzone.taz_sub


@orca.column('parcels', cache=True, cache_scope='iteration')
def cml(parcels, mandatory_accessibility,
        accessibilities_segmentation):
    mand_acc = mandatory_accessibility.local
    acc_seg = accessibilities_segmentation.local
    cols_to_sum = []
    for col in mand_acc.columns[~mand_acc.columns.isin(['destChoiceAlt',
                                                        'taz', 'subzone',
                                                        'weighted_sum'])]:
        if col in acc_seg.columns:
            mand_acc[col] = ((mand_acc[col] - mand_acc[col].min()) /
                             0.0134) * acc_seg.loc[0, col]
            cols_to_sum.append(col)
    mand_acc['weighted_sum'] = mand_acc[cols_to_sum].sum(axis=1)
    df = misc.reindex(mand_acc.weighted_sum, parcels.subzone)
    return df.reindex(parcels.index).fillna(-1)


@orca.column('parcels', cache=True, cache_scope='iteration')
def cnml(parcels, non_mandatory_accessibility,
         accessibilities_segmentation):
    nmand_acc = non_mandatory_accessibility.local
    acc_seg = accessibilities_segmentation.local
    cols_to_sum = []
    for col in nmand_acc.columns[~nmand_acc.columns.isin(['destChoiceAlt',
                                                          'taz', 'subzone',
                                                          'weighted_sum'])]:
        if col in acc_seg.columns:
            nmand_acc[col] = ((nmand_acc[col] - nmand_acc[col].min()) /
                              0.0175) * acc_seg.loc[0, col]
            cols_to_sum.append(col)
    nmand_acc['weighted_sum'] = nmand_acc[cols_to_sum].sum(axis=1)
    df = misc.reindex(nmand_acc.weighted_sum, parcels.subzone)
    return df.reindex(parcels.index).fillna(-1)


@orca.column('parcels', cache=True, cache_scope='iteration')
def combo_logsum(parcels):
    df = parcels.to_frame(['cml', 'cnml'])
    combo = df.cml + df.cnml
    # since the logsum methodology in the travel model has changed,
    # we need to shift the values to align with the ones used for estimation
    combo = combo - 170
    combo.loc[(combo <= 0)] = 1
    return combo


@orca.column('zones', cache=True, cache_scope='iteration')
def zone_cml(year, mandatory_accessibility,
             accessibilities_segmentation):
    mand_acc = mandatory_accessibility.local
    acc_seg = accessibilities_segmentation.local
    mand_acc = mand_acc.groupby('taz').median()
    cols_to_sum = []
    for col in mand_acc.columns[~mand_acc.columns.isin(['destChoiceAlt',
                                                        'taz', 'subzone',
                                                        'weighted_sum'])]:
        if col in acc_seg.columns:
            mand_acc[col] = ((mand_acc[col] - mand_acc[col].min()) /
                             0.0134) * acc_seg.loc[0, col]
            cols_to_sum.append(col)
        mand_acc['weighted_sum'] = mand_acc[cols_to_sum].sum(axis=1)
    return mand_acc['weighted_sum']


@orca.column('zones', cache=True, cache_scope='iteration')
def zone_cnml(year, non_mandatory_accessibility,
              accessibilities_segmentation):
    nmand_acc = non_mandatory_accessibility.local
    acc_seg = accessibilities_segmentation.local
    nmand_acc = nmand_acc.groupby('taz').median()
    cols_to_sum = []
    for col in nmand_acc.columns[~nmand_acc.columns.isin(['destChoiceAlt',
                                                          'taz', 'subzone',
                                                          'weighted_sum'])]:
        if col in acc_seg.columns:
            nmand_acc[col] = ((nmand_acc[col] - nmand_acc[col].min()) /
                              0.0175) * acc_seg.loc[0, col]
            cols_to_sum.append(col)
    nmand_acc['weighted_sum'] = nmand_acc[cols_to_sum].sum(axis=1)
    return nmand_acc['weighted_sum']


@orca.column('zones', cache=True, cache_scope='iteration')
def zone_combo_logsum(zones):
    df = zones.to_frame(['zone_cml', 'zone_cnml'])
    combo = df.zone_cml + df.zone_cnml
    # since the logsum methodology in the travel model has changed,
    # we need to shift the values to align with the ones used for estimation
    combo = combo - 170
    combo.loc[(combo <= 0)] = 1
    return combo


@orca.column('zones')
def ave_unit_sqft(buildings):
    return buildings.sqft_per_unit.groupby(buildings.zone_id).quantile(.6)


GROSS_AVE_UNIT_SIZE = 1000.0
PARCEL_USE_EFFICIENCY = .8
HEIGHT_PER_STORY = 12.0

###################################
#   Zoning Capacity Variables
###################################


@orca.column('parcels_zoning_calculations', cache=True)
def zoned_du(parcels, parcels_zoning_calculations):
    return parcels_zoning_calculations.effective_max_dua * parcels.parcel_acres


@orca.column('parcels_zoning_calculations', cache=True)
def zoned_du_vacant(parcels, parcels_zoning_calculations):
    return parcels_zoning_calculations.effective_max_dua * \
        parcels.parcel_acres * ~parcels.nodev * (parcels.total_sqft == 0)


@orca.column('parcels_zoning_calculations', cache=True)
def effective_max_dua(zoning_existing, parcels):

    max_dua_from_far = zoning_existing.max_far * 43560 / GROSS_AVE_UNIT_SIZE

    max_far_from_height = (zoning_existing.max_height / HEIGHT_PER_STORY) * PARCEL_USE_EFFICIENCY

    max_dua_from_height = max_far_from_height * 43560 / GROSS_AVE_UNIT_SIZE

    s = pd.concat([zoning_existing.max_dua, max_dua_from_far, max_dua_from_height], axis=1).min(axis=1)

    # take the max dua IFF the upzone value is greater than the current value
    # i.e. don't let the upzoning operation accidentally downzone

    strategy_max_dua = orca.get_table("zoning_strategy").dua_up

    s = pd.concat([s, strategy_max_dua], axis=1).max(axis=1)

    # take the min dua IFF the upzone value is less than the current value
    # i.e. don't let the downzoning operation accidentally upzone

    strategy_min_dua = orca.get_table("zoning_strategy").dua_down

    s = pd.concat([
        s,
        strategy_min_dua
    ], axis=1).min(axis=1)

    s3 = parcel_is_allowed('residential')

    return (s.fillna(0) * s3).reindex(parcels.index).fillna(0).astype('float')


@orca.column('parcels_zoning_calculations', cache=True)
def effective_max_far(zoning_existing, parcels):

    max_far_from_height = (zoning_existing.max_height / HEIGHT_PER_STORY) * PARCEL_USE_EFFICIENCY

    s = pd.concat([zoning_existing.max_far, max_far_from_height], axis=1).min(axis=1)

    # take the max far IFF the upzone value is greater than the current value
    # i.e. don't let the upzoning operation accidentally downzone

    strategy_max_far = orca.get_table("zoning_strategy").far_up

    s = pd.concat([s, strategy_max_far], axis=1).max(axis=1)

    # take the max far IFF the downzone value is less than the current value
    # i.e. don't let the downzoning operation accidentally upzone

    strategy_min_far = orca.get_table("zoning_strategy").far_down

    s = pd.concat([s, strategy_min_far], axis=1).min(axis=1)

    return s.reindex(parcels.index).fillna(0).astype('float')


@orca.column('parcels_zoning_calculations', cache=True)
def effective_max_office_far(parcels_zoning_calculations):
    return parcels_zoning_calculations.effective_max_far * \
        parcel_is_allowed('office')


########################################
# there are a number of variables
# here that try to get at zoned capacity
########################################


@orca.column('parcels_zoning_calculations')
def zoned_du_underbuild(parcels, parcels_zoning_calculations):
    # subtract from zoned du, the total res units, but also the equivalent
    # of non-res sqft in res units
    s = (parcels_zoning_calculations.zoned_du -
         parcels.total_residential_units -
         parcels.total_non_residential_sqft /
         GROSS_AVE_UNIT_SIZE).clip(lower=0)
    ratio = (s / parcels.total_residential_units).replace(np.inf, 1)
    # if the ratio of additional units to existing units is not at least .5
    # we don't build it - I mean we're not turning a 10 story building into an
    # 11 story building
    s = s[ratio > .5].reindex(parcels.index).fillna(0)
    return s.astype('int')


@orca.column('parcels_zoning_calculations')
def zoned_du_build_ratio(parcels, parcels_zoning_calculations):
    # ratio of existing res built space to zoned res built space
    s = parcels.total_residential_units / \
       (parcels_zoning_calculations.effective_max_dua * parcels.parcel_acres)
    return s.replace(np.inf, 1).clip(0, 1)


@orca.column('parcels_zoning_calculations')
def zoned_far_build_ratio(parcels, parcels_zoning_calculations):
    # ratio of existing nonres built space to zoned nonres built space
    s = parcels.total_non_residential_sqft / \
        (parcels_zoning_calculations.effective_max_far *
         parcels.parcel_size)
    return s.replace(np.inf, 1).clip(0, 1)


@orca.column('parcels_zoning_calculations')
def zoned_build_ratio(parcels_zoning_calculations):
    # add them together in order to get the sum of residential and commercial
    # build space
    return parcels_zoning_calculations.zoned_du_build_ratio + \
        parcels_zoning_calculations.zoned_far_build_ratio
