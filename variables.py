import numpy as np
import pandas as pd
from urbansim.utils import misc
import orca
import datasources
from urbansim_defaults import utils
from urbansim_defaults import variables


#####################
# HOUSEHOLDS VARIABLES
#####################


# used to pretent a segmented choice model isn't actually segmented
@orca.column('households', 'ones', cache=True)
def income_decile(households):
    return pd.Series(1, households.index)


@orca.column('households', 'tmnode_id', cache=True)
def node_id(households, buildings):
    return misc.reindex(buildings.tmnode_id, households.building_id)


#####################
# HOMESALES VARIABLES
#####################


@orca.column('homesales', 'juris_ave_income', cache=True)
def juris_ave_income(parcels, homesales):
    return misc.reindex(parcels.juris_ave_income, homesales.parcel_id)


@orca.column('homesales', 'is_sanfran', cache=True)
def is_sanfran(parcels, homesales):
    return misc.reindex(parcels.is_sanfran, homesales.parcel_id)


@orca.column('homesales', 'node_id', cache=True)
def node_id(homesales, parcels):
    return misc.reindex(parcels.node_id, homesales.parcel_id)


@orca.column('homesales', 'tmnode_id', cache=True)
def tmnode_id(homesales, parcels):
    return misc.reindex(parcels.tmnode_id, homesales.parcel_id)


@orca.column('homesales', 'zone_id', cache=True)
def zone_id(homesales, parcels):
    return misc.reindex(parcels.zone_id, homesales.parcel_id)


@orca.column('homesales', cache=True)
def modern_condo(homesales):
    # this is to try and differentiate between new
    # construction in the city vs in the burbs
    return ((homesales.year_built > 2000) *
            (homesales.building_type_id == 3)).astype('int')


@orca.column('homesales', cache=True)
def base_price_per_sqft(homesales):
    s = homesales.price_per_sqft.groupby(homesales.zone_id).quantile()
    return misc.reindex(s, homesales.zone_id)


@orca.column('homesales', cache=True)
def transit_type(homesales, parcels_geography):
    return misc.reindex(parcels_geography.tpp_id, homesales.parcel_id).\
        reindex(homesales.index).fillna('none')


#####################
# COSTAR VARIABLES
#####################


@orca.column('costar', 'juris_ave_income', cache=True)
def juris_ave_income(parcels, costar):
    return misc.reindex(parcels.juris_ave_income, costar.parcel_id)


@orca.column('costar', 'is_sanfran', cache=True)
def is_sanfran(parcels, costar):
    return misc.reindex(parcels.is_sanfran, costar.parcel_id)


@orca.column('costar', 'general_type')
def general_type(costar):
    return costar.PropertyType


@orca.column('costar', 'node_id')
def node_id(parcels, costar):
    return misc.reindex(parcels.node_id, costar.parcel_id)


@orca.column('costar', 'tmnode_id')
def tmnode_id(parcels, costar):
    return misc.reindex(parcels.tmnode_id, costar.parcel_id)


@orca.column('costar', 'zone_id')
def zone_id(parcels, costar):
    return misc.reindex(parcels.zone_id, costar.parcel_id)


@orca.column('costar', cache=True)
def transit_type(costar, parcels_geography):
    return misc.reindex(parcels_geography.tpp_id, costar.parcel_id).\
        reindex(costar.index).fillna('none')


#####################
# JOBS VARIABLES
#####################


@orca.column('jobs', 'tmnode_id', cache=True)
def tmnode_id(jobs, buildings):
    return misc.reindex(buildings.tmnode_id, jobs.building_id)


@orca.column('jobs', 'naics', cache=True)
def naics(jobs):
    return jobs.sector_id


@orca.column('jobs', 'empsix', cache=True)
def empsix(jobs, settings):
    return jobs.naics.map(settings['naics_to_empsix'])


@orca.column('jobs', 'empsix_id', cache=True)
def empsix_id(jobs, settings):
    return jobs.empsix.map(settings['empsix_name_to_id'])


@orca.column('jobs', 'preferred_general_type')
def preferred_general_type(jobs, buildings, settings):
    # this column is the preferred general type for this job - this is used
    # in the non-res developer to determine how much of a building type to
    #  use.  basically each job has a certain pdf by sector of the building
    # types is prefers and so we give each job a preferred type based on that
    # pdf.  note that if a job is assigned a building, the building's type is
    # it's preferred type by definition

    s = misc.reindex(buildings.general_type, jobs.building_id).fillna("Other")

    sector_pdfs = pd.DataFrame(settings['job_sector_to_type'])
    # normalize (to be safe)
    sector_pdfs = sector_pdfs / sector_pdfs.sum()

    for sector in sector_pdfs.columns:
        mask = ((s == "Other") & (jobs.empsix == sector))

        sector_pdf = sector_pdfs[sector]
        s[mask] = np.random.choice(sector_pdf.index,
                                   size=mask.value_counts()[True],
                                   p=sector_pdf.values)

    return s


#####################
# BUILDINGS VARIABLES
#####################


@orca.column('buildings', cache=True)
def transit_type(buildings, parcels_geography):
    return misc.reindex(parcels_geography.tpp_id, buildings.parcel_id).\
        reindex(buildings.index).fillna('none')


@orca.column('buildings', cache=True)
def base_price_per_sqft(homesales, buildings):
    s = homesales.price_per_sqft.groupby(homesales.zone_id).quantile()
    return misc.reindex(s, buildings.zone_id).reindex(buildings.index)\
        .fillna(s.quantile())


@orca.column('buildings', 'tmnode_id', cache=True)
def tmnode_id(buildings, parcels):
    return misc.reindex(parcels.tmnode_id, buildings.parcel_id)


@orca.column('buildings', 'juris_ave_income', cache=True)
def juris_ave_income(parcels, buildings):
    return misc.reindex(parcels.juris_ave_income, buildings.parcel_id)


@orca.column('buildings', 'is_sanfran', cache=True)
def is_sanfran(parcels, buildings):
    return misc.reindex(parcels.is_sanfran, buildings.parcel_id)


@orca.column('buildings', 'sqft_per_unit', cache=True)
def unit_sqft(buildings):
    return (buildings.building_sqft /
            buildings.residential_units.replace(0, 1)).clip(400, 6000)


@orca.column('buildings', cache=True)
def modern_condo(buildings):
    # this is to try and differentiate between new construction
    # in the city vs in the burbs
    return ((buildings.year_built > 2000) * (buildings.building_type_id == 3))\
        .astype('int')


#####################
# PARCELS VARIABLES
#####################


@orca.column('parcels', cache=True)
def pda(parcels, parcels_geography):
    return parcels_geography.pda_id.reindex(parcels.index)


@orca.column('parcels', cache=True)
def juris(parcels, parcels_geography):
    return parcels_geography.juris_name.reindex(parcels.index)


# these are actually functions that take parameters, but are parcel-related
# so are defined here
@orca.injectable('parcel_average_price', autocall=False)
def parcel_average_price(use, quantile=.5):
    # I'm testing out a zone aggregation rather than a network aggregation
    # because I want to be able to determine the quantile of the distribution
    # I also want more spreading in the development and not keep it localized
    if use == "residential":
        buildings = orca.get_table('buildings')
        # get price per sqft
        s = buildings.residential_price * 1.3  # / buildings.sqft_per_unit
        # limit to res
        s = s[buildings.general_type == "Residential"]
        # group by zoneid and get 80th percentile
        s = s.groupby(buildings.zone_id).quantile(.8).clip(150, 1250)
        # broadcast back to parcel's index
        s = misc.reindex(s, orca.get_table('parcels').zone_id)
        # shifters
        cost_shifters = orca.get_table("parcels").cost_shifters
        price_shifters = orca.get_table("parcels").price_shifters
        s = s / cost_shifters * price_shifters
        # just to make sure
        s = s.fillna(0).clip(150, 1250)
        return s

    if 'nodes' not in orca.list_tables():
        return pd.Series(0, orca.get_table('parcels').index)

    return misc.reindex(orca.get_table('nodes')[use],
                        orca.get_table('parcels').node_id)


@orca.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    if use == "residential":
        s *= 1.0
    return s


@orca.injectable('parcel_is_allowed_func', autocall=False)
def parcel_is_allowed(form):
    settings = orca.get_injectable('settings')
    form_to_btype = settings["form_to_btype"]
    # we have zoning by building type but want
    # to know if specific forms are allowed
    allowed = [orca.get_table('zoning_baseline')
               ['type%d' % typ] > 0 for typ in form_to_btype[form]]
    s = pd.concat(allowed, axis=1).max(axis=1).\
        reindex(orca.get_table('parcels').index).fillna(False)

    return s

@orca.column('parcels','first_building_type_id')
def first_building_type_id(buildings, parcels):
    df = buildings.to_frame(columns=['building_type_id','parcel_id','general_type']).groupby('parcel_id').first() #hack
    df1 = parcels.to_frame(columns=['parcel_acres','zone_id'])
    df1['first_building_type_id'] = df['building_type_id']
    s = df1['first_building_type_id']
    return s

@orca.injectable('parcel_first_building_type_is', autocall=False)
def parcel_first_building_type_is(form):
    settings = orca.get_injectable('settings')
    form_to_btype = settings["form_to_btype"]
    parcels = orca.get_table('parcels')
    s = parcels.first_building_type_id.isin(form_to_btype[form])
    return s

@orca.column('zones_tm_output','gqpop')
def gqpop(year, zones, zone_forecast_inputs):
    str1 = "gqpop" + str(year)[-2:]
    s = zone_forecast_inputs[str1]
    return s

@orca.column('zones_tm_output','totemp')
def totemp(jobs):
    s = jobs.groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','tothh')
def tothh(households):
    s = households.groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','hhpop')
def hhpop(households):
    s = zones['hhpop'] = households.groupby('zone_id').persons.sum()
    return s

@orca.column('zones_tm_output','resunits')
def resunits(buildings):
    s = buildings.groupby('zone_id').residential_units.sum()
    return s

@orca.column('zones_tm_output','resvacancy')
def resvacancy(households):
    s = (zones.resunits - zones.tothh) / \
    zones.resunits.replace(0, 1)
    return s

@orca.column('zones_tm_output','mfdu')
def mfdu(buildings):
    s = \
    buildings.query("building_type_id == 3 or building_type_id == 12").\
    groupby('zone_id').residential_units.sum()
    return s

@orca.column('zones_tm_output','sfdu')
def hhpop(buildings):
    s = \
    buildings.query("building_type_id == 1 or building_type_id == 2").\
    groupby('zone_id').residential_units.sum()
    return s

@orca.column('zones_tm_output','hhinq1')
def hhinq1(households):
    s = households.query("income < 25000").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','hhinq2')
def hhinq2(households):
    s = households.query("income >= 25000 and income < 45000").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','hhinq3')
def hhinq3(households):
    s = households.query("income >= 45000 and income < 75000").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','hhinq4')
def hhinq4(households):
    s = households.query("income >= 75000").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','newdevacres')
def newdevacres(buildings):
    s = (buildings.query("building_sqft > 0").
    groupby('zone_id').lot_size_per_unit.sum()) / 43560
    return s

@orca.column('zones_tm_output','agrempn')
def agrempn(jobs):
    s = jobs.query("empsix == 'AGREMPN'").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','mwtempn')
def hhpop(jobs):
    s = jobs.query("empsix == 'MWTEMPN'").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','retempn')
def hhpop(jobs):
    s = jobs.query("empsix == 'RETEMPN'").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','fsempn')
def fsempn(jobs):
    s = jobs.query("empsix == 'FPSEMPN'").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','herempn')
def herempn(jobs):
    s = jobs.query("empsix == 'HEREMPN'").\
        groupby('zone_id').size()
    return s

@orca.column('zones_tm_output','othempn')
def hhpop(jobs):
    s = jobs.query("empsix == 'OTHEMPN'").\
        groupby('zone_id').size()
    return s

# @orca.column('zones_tm_output','hhpop')
# def hhpop(households):
#     return s

# @orca.column('zones_tm_output','hhpop')
# def hhpop(households):
#     return s

# @orca.column('zones_tm_output','hhpop')
# def hhpop(households):
#     return s

# @orca.column('zones_tm_output','hhpop')
# def hhpop(households):
#     return s

# @orca.column('zones','density')
#Density = (Total Population + 2.5 * Total Employment) / (Total Acres) 

@orca.column('zones_tm_output','ciacre')
def ciacre(parcels, zones_tm_output):
    f = orca.get_injectable('parcel_first_building_type_is')
    s = f('select_non_residential')
    s1 = parcels.get_column('zone_id')
    s2 = parcels.parcel_acres*s
    df = pd.DataFrame(data={'zone_id':s1,'ciacre':s2}) #'commercial_industrial_acres':s2
    s3 = df.groupby('zone_id').ciacre.sum()
    return s3

@orca.column('zones_tm_output','resacre')
def resacre(parcels):
    f = orca.get_injectable('parcel_first_building_type_is')
    s = f('residential') | f('mixedresidential')
    s1 = parcels.get_column('zone_id')
    s2 = parcels.parcel_acres*s
    df = pd.DataFrame(data={'zone_id':s1,'residential_acres':s2}) #'commercial_industrial_acres':s2
    s3 = df.groupby('zone_id').residential_acres.sum()
    return s3

@orca.column('parcels', 'juris_ave_income', cache=True)
def juris_ave_income(households, buildings, parcels_geography, parcels):
    h = orca.merge_tables("households",
                          [households, buildings, parcels_geography],
                          columns=["jurisdiction", "income"])
    s = h.groupby(h.jurisdiction).income.quantile(.5)
    return misc.reindex(s, parcels_geography.jurisdiction).\
        reindex(parcels.index).fillna(s.median()).apply(np.log1p)


# returns the newest building on the land and fills missing values with 1800 -
# for use with development limits
@orca.column('parcels', 'newest_building')
def newest_building(parcels, buildings):
    return buildings.year_built.groupby(buildings.parcel_id).max().\
        reindex(parcels.index).fillna(1800)


@orca.column('parcels', cache=True)
def manual_nodev(parcel_rejections, parcels):
    df1 = parcels.to_frame(['x', 'y']).dropna(subset=['x', 'y'])
    df2 = parcel_rejections.to_frame(['lng', 'lat'])
    df2 = df2[parcel_rejections.state == "denied"]
    df2 = df2[["lng", "lat"]]  # need to change the order
    ind = datasources.nearest_neighbor(df1, df2)

    s = pd.Series(False, parcels.index)
    s.loc[ind.flatten()] = True
    return s.astype('int')


@orca.column('parcels', 'oldest_building_age')
def oldest_building_age(parcels, year):
    return year - parcels.oldest_building.replace(9999, 0)


@orca.column('parcels', 'is_sanfran', cache=True)
def is_sanfran(parcels_geography, buildings, parcels):
    return (parcels_geography.juris_name == "San Francisco").\
        reindex(parcels.index).fillna(False).astype('int')


# actual columns start here
@orca.column('parcels', 'max_far', cache=True)
def max_far(parcels, scenario, scenario_inputs):
    s = utils.conditional_upzone(scenario, scenario_inputs,
                                 "max_far", "far_up").\
        reindex(parcels.index)
    return s * ~parcels.nodev


# returns a vector where parcels are ALLOWED to be built
@orca.column('parcels')
def parcel_rules(parcels):
    # removes parcels with buildings < 1940,
    # and single family homes on less then half an acre
    s = (parcels.oldest_building < 1940) | \
        ((parcels.total_residential_units == 1) & (parcels.parcel_acres < .5))
    s = (~s.reindex(parcels.index).fillna(False)).astype('int')
    return s

@orca.column('parcels', 'total_non_residential_sqft', cache=True)
def total_non_residential_sqft(parcels, buildings):
    return buildings.non_residential_sqft.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)

@orca.column('parcels')
def nodev(zoning_baseline, parcels):
    return zoning_baseline.nodev.reindex(parcels.index).\
        fillna(0).astype('bool')


@orca.column('parcels', 'max_dua', cache=True)
def max_dua(parcels, scenario, scenario_inputs):
    s = utils.conditional_upzone(scenario, scenario_inputs,
                                 "max_dua", "dua_up").\
        reindex(parcels.index)
    return s * ~parcels.nodev


@orca.column('parcels', 'max_height', cache=True)
def max_height(parcels, zoning_baseline):
    return zoning_baseline.max_height.reindex(parcels.index)


# these next two are just indicators put into the output
@orca.column('parcels', 'residential_purchase_price_sqft')
def residential_purchase_price_sqft(parcels):
    return parcels.building_purchase_price_sqft


@orca.column('parcels', 'residential_sales_price_sqft')
def residential_sales_price_sqft(parcel_sales_price_sqft_func):
    return parcel_sales_price_sqft_func("residential")


@orca.column('parcels', 'general_type')
def general_type(parcels, buildings):
    s = buildings.general_type.groupby(buildings.parcel_id).first()
    return s.reindex(parcels.index).fillna("Vacant")


# for debugging reasons this is split out into its own function
@orca.column('parcels', 'building_purchase_price_sqft')
def building_purchase_price_sqft(parcels):
    price = pd.Series(0, parcels.index)
    gentype = parcels.general_type
    for form in ["Office", "Retail", "Industrial", "Residential"]:
        # convert to yearly
        factor = 1.0 if form == "Residential" else 20.0
        # raise cost to convert from industrial
        if form == "Industrial":
            factor *= 3.0
        if form == "Retail":
            factor *= 2.0
        tmp = parcel_average_price(form.lower())
        price += tmp * (gentype == form) * factor

    return price.clip(150, 2500)


@orca.column('parcels', 'building_purchase_price')
def building_purchase_price(parcels):
    # the .8 is because we assume the building only charges for 80% of the
    # space - when a good portion of the building is parking, this probably
    # overestimates the part of the building that you can charge for
    # the second .85 is because the price is likely to be lower for existing
    # buildings than for one that is newly built
    return (parcels.total_sqft * parcels.building_purchase_price_sqft *
            .8 * .85).reindex(parcels.index).fillna(0)


@orca.column('parcels', 'land_cost')
def land_cost(parcels):
    s = pd.Series(20, parcels.index)
    s[parcels.general_type == "Industrial"] = 150.0
    return parcels.building_purchase_price + parcels.parcel_size * s


@orca.column('parcels', 'county')
def county(parcels, settings):
    return parcels.county_id.map(settings["county_id_map"])


@orca.column('parcels', 'cost_shifters')
def cost_shifters(parcels, settings):
    return parcels.county.map(settings["cost_shifters"])


@orca.column('parcels', 'price_shifters')
def price_shifters(parcels, settings):
    return parcels.pda.map(settings["pda_price_shifters"]).fillna(1.0)


@orca.column('parcels', 'node_id', cache=True)
def node_id(parcels, net):
    s = net["walk"].get_node_ids(parcels.x, parcels.y)
    fill_val = s.value_counts().index[0]
    s = s.reindex(parcels.index).fillna(fill_val).astype('int')
    return s


@orca.column('parcels', 'tmnode_id', cache=True)
def node_id(parcels, net):
    s = net["drive"].get_node_ids(parcels.x, parcels.y)
    fill_val = s.value_counts().index[0]
    s = s.reindex(parcels.index).fillna(fill_val).astype('int')
    return s

GROSS_AVE_UNIT_SIZE = 1000

####################################
#####Zoning Capacity Variables######
####################################

@orca.column('parcels_zoning_calculations', 'zoned_du', cache=True)
def zoned_du(parcels):
    s = parcels.max_dua * parcels.parcel_acres
    s2 = parcels.max_far * parcels.parcel_size / GROSS_AVE_UNIT_SIZE
    s3 = parcel_is_allowed('residential') # consider including: | parcel_is_allowed('mixedresidential')
    return (s.fillna(s2)*s3).reindex(parcels.index).fillna(0).astype('int')

@orca.column('parcels_zoning_calculations', 'effective_max_dua', cache=True)
def effective_max_dua(parcels):
    s = parcels.max_dua
    s2 = parcels.max_far * parcels.parcel_size / GROSS_AVE_UNIT_SIZE / parcels.parcel_acres # not elegant, b/c GROSS_AVE_UNIT_SIZE is actually Area/DU already
    s3 = parcel_is_allowed('residential') # consider including: | parcel_is_allowed('mixedresidential')
    return (s.fillna(s2)*s3).reindex(parcels.index).fillna(0).astype('float')

@orca.column('parcels_zoning_calculations', 'effective_max_office_far', cache=True)
def effective_max_office_far(parcels):
    s = parcels.max_far
    s2 = parcels.max_height / 11 # assuming 12ft floor to floor ratio for office building height
    s3 = parcel_is_allowed('office')
    return (s.fillna(s2)*s3).reindex(parcels.index).fillna(0).astype('float')

# there are a number of variables here that try to get at zoned capacity
@orca.column('parcels_zoning_calculations', 'zoned_du_underbuild')
def zoned_du_underbuild(parcels, parcels_zoning_calculations):
    # subtract from zoned du, the total res units, but also the equivalent
    # of non-res sqft in res units
    s = (parcels_zoning_calculations.zoned_du - parcels.total_residential_units -
         parcels.total_non_residential_sqft / GROSS_AVE_UNIT_SIZE)\
         .clip(lower=0)
    ratio = (s / parcels.total_residential_units).replace(np.inf, 1)
    # if the ratio of additional units to existing units is not at least .5
    # we don't build it - I mean we're not turning a 10 story building into an
    # 11 story building
    s = s[ratio > .5].reindex(parcels.index).fillna(0)
    return s.astype('int')

@orca.column('parcels_zoning_calculations')
def zoned_du_underbuild_nodev(parcels, parcels_zoning_calculations):
    return (parcels_zoning_calculations.zoned_du_underbuild * parcels.parcel_rules).astype('int')

@orca.column('parcels_zoning_calculations','office_allowed')
def office_allowed(parcels):
    office_allowed = parcel_is_allowed('office')
    return office_allowed

@orca.column('parcels_zoning_calculations','retail_allowed')
def retail_allowed(parcels):
    retail_allowed = parcel_is_allowed('retail')
    return retail_allowed

@orca.column('parcels_zoning_calculations','industrial_allowed')
def industrial_allowed(parcels):
    industrial_allowed = parcel_is_allowed('industrial')
    return industrial_allowed

###per: https://www.pivotaltracker.com/n/projects/1391722/stories/105696918
#We want to build a map that shows one value per parcel representing its non-residential zoning situation. Follow these rules in order to classify every parcel:
#1) If a parcel allows office development with an FAR > 4 it is tagged OH
#2) If a parcel allows office development with an FAR >1 and <=4 it is tagged OM
#3) If a parcel allows office development with an FAR <=1 it is tagged OL
#4) If a parcel disallows office development and allows retail development it is tagged R
#5) If a parcel disallows office and retail development and allows industrial development it is tagged I
#6) remaining untagged parcels are left with no value

@orca.column('parcels_zoning_calculations','cat_r')
def cat_r(parcels_zoning_calculations):
    s = ~parcels_zoning_calculations.office_allowed&\
        parcels_zoning_calculations.retail_allowed
    s2 = pd.Series(index=parcels_zoning_calculations.index).fillna('R')
    return s*s2

@orca.column('parcels_zoning_calculations','cat_ind')
def cat_ind(parcels_zoning_calculations):
    s = ~parcels_zoning_calculations.office_allowed&\
        ~parcels_zoning_calculations.retail_allowed&\
        parcels_zoning_calculations.industrial_allowed
    s2 = pd.Series(index=parcels_zoning_calculations.index).fillna('I')
    return s*s2

@orca.column('parcels_zoning_calculations','office_high')
def office_high(parcels_zoning_calculations):
    s = parcels_zoning_calculations.effective_max_office_far > 4
    s2 = pd.Series(index=parcels_zoning_calculations.index).fillna('OH')
    s3 = s*s2
    return s3

@orca.column('parcels_zoning_calculations','office_medium')
def office_medium(parcels_zoning_calculations):
    s = parcels_zoning_calculations.effective_max_office_far > 1
    s2 = parcels_zoning_calculations.effective_max_office_far <= 4
    s3 = pd.Series(index=parcels_zoning_calculations.index).fillna('OM')
    return (s&s2)*s3

@orca.column('parcels_zoning_calculations','office_low')
def office_low(parcels_zoning_calculations):
    s = parcels_zoning_calculations.effective_max_office_far < 1
    s2 = parcels_zoning_calculations.office_allowed
    s3 = pd.Series(index=parcels_zoning_calculations.index).fillna('OL')
    return (s&s2)*s3

@orca.column('parcels_zoning_calculations','non_res_categories')
def non_res_categories(parcels_zoning_calculations):
    pzc = parcels_zoning_calculations
    s = pzc.office_high+pzc.office_medium+pzc.office_low+pzc.cat_r+pzc.cat_ind
    return s
