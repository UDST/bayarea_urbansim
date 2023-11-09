#
# Create 2015 tazdata map from UrbanSim input layer(s) using building data and pipeline data
# Reads
#  1) UrbanSim basemap h5 (URBANSIM_BASEMAP_FILE), parcels and buildings
#  2) Development pipeline csv (URBANSIM_BASEMAP_FILE)
#  3) Employment taz data csv (EMPLOYMENT_FILE)
#
# Outputs
# 
# Notes:
#  - zone_id and county/county_id aren't always consistent with the TM mapping between zones/county
#    (https://github.com/BayAreaMetro/travel-model-one/blob/master/utilities/geographies/taz-superdistrict-county.csv)
#    This script assumes the zone_id is accurate and pull the county from the TM correspondence file
#    TODO: report on these?
#
# for arcpy:
# set PATH=C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3

import logging,os,re,sys,time
import numpy, pandas

NOW = time.strftime("%Y%b%d.%H%M")

# taz-county file
TAZ_COUNTY_FILE       = "X:\\travel-model-one-master\\utilities\\geographies\\taz-superdistrict-county.csv"

# taz shapefile
TAZ_SHPFILE = "M:\\Data\\GIS layers\\TM1_taz\\bayarea_rtaz1454_rev1_WGS84.shp"


# reference for creation: 
# Create and share 2015 tazdata from basemap plus development pipeline with MTC planners @
#   https://app.asana.com/0/385259290425521/1165636787387665/f
if os.getenv("USERNAME")=="lzorn":
    # use local dir to make things faster
    URBANSIM_LOCAL_DIR    = "C:\\Users\\lzorn\\Documents\\UrbanSim_InputMapping"
    # from https://mtcdrive.box.com/s/w0fmrz85l9cti2byd6rjqu9hv0m2edlq
    URBANSIM_BASEMAP_FILE = "2020_03_20_bayarea_v6.h5"
    # from https://mtcdrive.box.com/s/wcxlgwov5l6s6p0p0vh2xj1ekdynxxw5
    URBANSIM_PIPELINE_FILE= "pipeline_2020Mar20.1512.csv"
    URBANSIM_PIPELINE_GDB = "devproj_2020Mar20.1512.gdb"

    # employment data
    EMPLOYMENT_FILE       = "X:\\petrale\\applications\\travel_model_lu_inputs\\2015\\TAZ1454 2015 Land Use.csv"
    OUTPUT_DIR            = os.path.join(URBANSIM_LOCAL_DIR, "map_data")
    LOG_FILE              = os.path.join(OUTPUT_DIR, "create_tazdata_devpipeline_map_{}.log".format(NOW))

    # building types
    BUILDING_TYPE_FILE    = "X:\\petrale\\incoming\\dv_buildings_det_type_lu.csv"
    # with activity categories
    BUILDING_TYPE_ACTIVITY_FILE = "X:\\petrale\\TableauAliases.xlsx"

    # geodatabase for arcpy and map
    WORKSPACE_GDB         = "C:\\Users\\lzorn\\Documents\\UrbanSim_InputMapping\\UrbanSim_InputMapping.gdb"
    ARCGIS_PROJECT        = "C:\\Users\\lzorn\\Documents\\UrbanSim_InputMapping\\UrbanSim_InputMapping.aprx"

# year buit categories we care about
#          name,  min, max
YEAR_BUILT_CATEGORIES = [
    ("0000-2000",   0,2000),
    ("2001-2010",2001,2010),
    ("2011-2015",2011,2015),
    ("2016-2020",2016,2020),
    ("2021-2030",2021,2030),
    ("2031-2050",2031,2050),
]
# aggregate
YEAR_BUILT_CATEGORIES_AGG = [
    ("0000-2015",   0,2015),
    ("2016-2050",2016,2050),
 ]

COUNTY_ID_NAME = [
    ("Alameda"      , 1),
    ("Contra Costa" ,13),
    ("Marin"        ,41),
    ("Napa"         ,55),
    ("San Francisco",75),
    ("San Mateo"    ,81),
    ("Santa Clara"  ,85),
    ("Solano"       ,95),
    ("Sonoma"       ,97),
]
COUNTY_ID_NAME_DF = pandas.DataFrame(COUNTY_ID_NAME, columns=["county","county_id"])

def set_year_built_category(df):
    # set year_built_category, year_built_category_agg columns based on YEAR_BUILT_CATEGORIES and year_built column
    df["year_built_category"] = "????-????"
    for category in YEAR_BUILT_CATEGORIES:
        CAT_NAME = category[0]
        YEAR_MIN = category[1]
        YEAR_MAX = category[2]

        df.loc[(df.year_built >= YEAR_MIN)&(df.year_built <= YEAR_MAX), "year_built_category"] = CAT_NAME

    df["year_built_category_agg"] = "????-????"
    for category in YEAR_BUILT_CATEGORIES_AGG:
        CAT_NAME = category[0]
        YEAR_MIN = category[1]
        YEAR_MAX = category[2]

        df.loc[(df.year_built >= YEAR_MIN)&(df.year_built <= YEAR_MAX), "year_built_category_agg"] = CAT_NAME

    return df

def warn_zone_county_disagreement(df):
    # check if zone/county mapping disagree with the TM mapping and log issues
    # TODO
    pass

if __name__ == '__main__':
    # pandas options
    pandas.options.display.max_rows = 999

    if not os.path.exists(OUTPUT_DIR): os.mkdir(OUTPUT_DIR)

    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')

    # console handler
    ch = logging.StreamHandler()
    ch.setLevel('INFO')
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(ch)
    # file handler
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setLevel('DEBUG')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(fh)

    logger.info("Output dir: {}".format(OUTPUT_DIR))

    ####################################
    taz_sd_county_df = pandas.read_csv(TAZ_COUNTY_FILE)
    logger.info("Read {}; head:\n{}".format(TAZ_COUNTY_FILE, taz_sd_county_df.head()))
    # let's just keep taz/county
    taz_sd_county_df = taz_sd_county_df[["ZONE","COUNTY_NAME", "SD_NAME", "SD_NUM_NAME"]]
    taz_sd_county_df.rename(columns={"ZONE":"zone_id", "COUNTY_NAME":"county"},inplace=True)
    # and county_id
    taz_sd_county_df = pandas.merge(left=taz_sd_county_df, right=COUNTY_ID_NAME_DF)
    logger.debug("taz_sd_county_df head:\n{}".format(taz_sd_county_df.head()))

    ####################################
    building_types_df = pandas.read_csv(BUILDING_TYPE_FILE, skipinitialspace=True)
    building_types_df.set_index("building_type_det", inplace=True)
    logger.info("Read {}:\n{}".format(BUILDING_TYPE_FILE, building_types_df))

    BUILDING_TYPE_TO_DESC = building_types_df["detailed description"].to_dict()
    BUILDING_TYPE_TO_DESC["all"] = "all"
    logger.debug("BUILDING_TYPE_TO_DESC: {}".format(BUILDING_TYPE_TO_DESC))

    building_activity_df = pandas.read_excel(BUILDING_TYPE_ACTIVITY_FILE, sheet_name="building_type")
    building_types_df = pandas.merge(left=building_types_df, right=building_activity_df,
                                     how="left", left_index=True, right_on="building_type_det")
    logger.debug("building_types_df: \n{}".format(building_types_df))

    ####################################
    tm_lu_df = pandas.read_csv(EMPLOYMENT_FILE)
    logger.info("Read {}; head:\n{}".format(EMPLOYMENT_FILE, tm_lu_df.head()))
    tm_lu_df.rename(columns={"ZONE":"zone_id"}, inplace=True)
    # keep only employment, tothh, totpop, hhpop
    tm_lu_df = tm_lu_df[["zone_id","TOTHH","TOTPOP","HHPOP","TOTEMP","RETEMPN","FPSEMPN","HEREMPN","AGREMPN","MWTEMPN","OTHEMPN"]]

    ####################################
    logger.info("Reading parcels and buildings from {}".format(os.path.join(URBANSIM_LOCAL_DIR, URBANSIM_BASEMAP_FILE)))

    # use this for parcel_id (index), county_id, zone_id, acres
    parcels_df   = pandas.read_hdf(os.path.join(URBANSIM_LOCAL_DIR, URBANSIM_BASEMAP_FILE), key='parcels')
    # logger.info(parcels_df.dtypes)
    parcels_df = parcels_df[["zone_id","acres"]].reset_index().rename(columns={"acres":"parcel_acres"})
    logger.info("parcels_df.head():\n{}".format(parcels_df.head()))

    # sum parcel acres to zone
    parcels_zone_df = parcels_df.groupby(["zone_id"]).agg({"parcel_acres":"sum"}).reset_index()
    logger.info("parcels_zone_df:\n{}".format(parcels_zone_df.head()))

    buildings_df = pandas.read_hdf(os.path.join(URBANSIM_LOCAL_DIR, URBANSIM_BASEMAP_FILE), key='buildings')
    logger.info("buildings_df.dtypes:\n{}".format(buildings_df.dtypes))
    #logger.info(buildings_df.head())

    # segment year buit to 0000-2000, 2001-2010, 2011-2015
    buildings_df = set_year_built_category(buildings_df)
    logger.info("buildings_df by year_built_category:\n{}".format(buildings_df["year_built_category"].value_counts()))

    # join buildings to parcel to get the zone
    buildings_df = pandas.merge(left=buildings_df, right=parcels_df[["parcel_id","zone_id"]], 
                                how="left", left_on=["parcel_id"], right_on=["parcel_id"])

    buildings_no_year_built = buildings_df.loc[pandas.isnull(buildings_df.year_built)]
    if len(buildings_no_year_built) > 0:
        logger.warn("buildings_df has {} rows with no year_built:\n{}".format(len(buildings_no_year_built), buildings_no_year_built))
    else:
        logger.info("buildings_df has 0 rows with no year_built")

    buildings_no_building_type = buildings_df.loc[pandas.isnull(buildings_df.building_type)]
    if len(buildings_no_building_type) > 0:
        logger.warn("buildings_df has {} rows with no building_type:\n{}".format(len(buildings_no_building_type), buildings_no_building_type))
    else:
        logger.info("buildings_df has 0 rows with no building_type")

    #### sum to zone by year_built_category and building_type: residential_units, residential_sqft, non_residential_sqft
    buildings_zone_btype_df = buildings_df.groupby(["zone_id","year_built_category_agg","year_built_category","building_type"]).agg(
                                {"residential_units"   :"sum",
                                 "building_sqft"       :"sum",
                                 "residential_sqft"    :"sum",
                                 "non_residential_sqft":"sum"})
    buildings_zone_btype_df.reset_index(inplace=True)
    buildings_zone_btype_df["source"] = "buildings"
    # reorder
    buildings_zone_btype_df = buildings_zone_btype_df[["zone_id","source",
        "year_built_category_agg","year_built_category","building_type",
        "residential_units","building_sqft","residential_sqft","non_residential_sqft"]]

    logger.info("buildings_zone_btype_df.head():\n{}".format(buildings_zone_btype_df.head()))
    logger.info("buildings_zone_btype_df.dtypes:\n{}".format(buildings_zone_btype_df.dtypes))

    #### sum to zone by year_built_category and NOT building_type: residential_units, residential_sqft, non_residential_sqft
    buildings_zone_df = buildings_df.groupby(["zone_id","year_built_category_agg","year_built_category"]).agg(
                                {"residential_units"   :"sum",
                                 "building_sqft"       :"sum",
                                 "residential_sqft"    :"sum",
                                 "non_residential_sqft":"sum"})
    buildings_zone_df.reset_index(inplace=True)
    buildings_zone_df["source"] = "buildings"
    buildings_zone_df["building_type"] = "all"

    # reorder
    buildings_zone_df = buildings_zone_df[list(buildings_zone_btype_df.columns.values)]

    logger.info("buildings_zone_df.head():\n{}".format(buildings_zone_df.head()))
    logger.info("buildings_zone_df.dtypes:\n{}".format(buildings_zone_df.dtypes))

    ####################################
    # read pipeline file
    logger.info("Reading pipeline from {}".format(os.path.join(URBANSIM_LOCAL_DIR, URBANSIM_PIPELINE_FILE)))
    pipeline_df = pandas.read_csv(os.path.join(URBANSIM_LOCAL_DIR, URBANSIM_PIPELINE_FILE))
    logger.info("pipeline_df.head():\n{}".format(pipeline_df.head()))
    logger.info("pipeline_df.dtypes:\n{}".format(pipeline_df.dtypes))
    # logger.info("pipeline_df by year_built:\n{}".format(pipeline_df["year_built"].value_counts()))
    pipeline_df = set_year_built_category(pipeline_df)
    logger.info("pipeline_df by year_built_category:\n{}".format(pipeline_df["year_built_category"].value_counts()))
    logger.info("pipeline_df by year_built_category_agg:\n{}".format(pipeline_df["year_built_category_agg"].value_counts()))

    pipeline_no_year_built = pipeline_df.loc[pandas.isnull(pipeline_df.year_built)]
    if len(pipeline_no_year_built) > 0:
        logger.warn("pipeline_df has {} rows with no year_built:\n{}".format(len(pipeline_no_year_built), pipeline_no_year_built))
    else:
        logger.info("pipeline_df has 0 rows with no year_built")

    pipeline_no_building_type = pipeline_df.loc[pandas.isnull(pipeline_df.building_type)]
    if len(pipeline_no_building_type) > 0:
        logger.warn("pipeline_df has {} rows with no building_type:\n{}".format(len(pipeline_no_building_type), pipeline_no_building_type))
    else:
        logger.info("pipeline_df has 0 rows with no building_type")

    # sum to zone by year_built_category and building_type
    # assume residential_sqft = building_sqft - non_residential_sqft
    pipeline_df["residential_sqft"] = pipeline_df["building_sqft"] - pipeline_df["non_residential_sqft"]

    #### sum to zone by year_built_category and building_type: residential_units, residential_sqft, non_residential_sqft
    pipeline_zone_btype_df = pipeline_df.groupby(["ZONE_ID","year_built_category_agg","year_built_category","building_type"]).agg(
                                {"residential_units"   :"sum",
                                 "building_sqft"       :"sum",
                                 "residential_sqft"    :"sum",
                                 "non_residential_sqft":"sum"})
    pipeline_zone_btype_df.reset_index(inplace=True)
    pipeline_zone_btype_df.rename(columns={"ZONE_ID":"zone_id"}, inplace=True)
    pipeline_zone_btype_df.loc[ pandas.isnull(pipeline_zone_btype_df.zone_id), "zone_id"] = 0 # null => 0
    pipeline_zone_btype_df.zone_id = pipeline_zone_btype_df.zone_id.astype(int)

    pipeline_zone_btype_df["source"] = "pipeline"
    pipeline_zone_btype_df = pipeline_zone_btype_df[list(buildings_zone_btype_df.columns)]
    logger.info("pipeline_zone_btype_df.head():\n{}".format(pipeline_zone_btype_df.head()))
    logger.info("pipeline_zone_btype_df.dtypes:\n{}".format(pipeline_zone_btype_df.dtypes))

    #### sum to zone by year_built_category and NOT building_type: residential_units, residential_sqft, non_residential_sqft
    pipeline_zone_df = pipeline_df.groupby(["ZONE_ID","year_built_category_agg","year_built_category"]).agg(
                                {"residential_units"   :"sum",
                                 "building_sqft"       :"sum",
                                 "residential_sqft"    :"sum",
                                 "non_residential_sqft":"sum"})
    pipeline_zone_df.reset_index(inplace=True)
    pipeline_zone_df.rename(columns={"ZONE_ID":"zone_id"}, inplace=True)
    pipeline_zone_df.loc[ pandas.isnull(pipeline_zone_df.zone_id), "zone_id"] = 0 # null => 0
    pipeline_zone_df.zone_id = pipeline_zone_df.zone_id.astype(int)

    pipeline_zone_df["source"] = "pipeline"
    pipeline_zone_df["building_type"] = "all"
    pipeline_zone_df = pipeline_zone_df[list(buildings_zone_btype_df.columns)]
    logger.info("pipeline_zone_df.head():\n{}".format(pipeline_zone_df.head()))
    logger.info("pipeline_zone_df.dtypes:\n{}".format(pipeline_zone_df.dtypes))


    ####################################
    # take buildings & pipeline by zone
    zone_df = pandas.concat([buildings_zone_btype_df, 
                             buildings_zone_df,
                             pipeline_zone_btype_df,
                             pipeline_zone_df], axis="index")
    logger.info("zone_df.head():\n{}".format(zone_df.head()))

    logger.debug("zone_df for zone_id=1: \n{}".format(zone_df.loc[zone_df.zone_id==1]))

    # pivot on buildings/pipeline including ALL building types
    zone_piv_df = zone_df.pivot_table(index  ="zone_id",
                                      columns=["source","year_built_category","building_type"],
                                      values =["residential_units", "building_sqft", "residential_sqft", "non_residential_sqft"],
                                      aggfunc=numpy.sum)
    logger.info("zone_piv_df.head():\n{}".format(zone_piv_df.head()))
    zone_piv_df.reset_index(inplace=True)

    logger.debug("zone_piv_df for zone_id=1: \n{}".format(zone_piv_df.loc[zone_piv_df.zone_id==1].squeeze()))

    # convert column names from tuples
    new_cols = []
    for col in zone_piv_df.columns.values:
        if col[1] == '':  # ('zone_id', '', '', '')
            new_cols.append(col[0])
        else: # ('building_sqft', 'buildings', '0000-2000', 'HM')
            new_cols.append(col[1]+" "+col[2]+" "+col[3]+" "+col[0])
    zone_piv_df.columns = new_cols
    logger.debug("zone_piv_df.head():\n{}".format(zone_piv_df.head()))
    logger.debug("zone_piv_df.dtypes:\n{}".format(zone_piv_df.dtypes))
    logger.debug("zone_piv_df.sum():\n{}".format(zone_piv_df.sum()))

    # pivot on buildings/pipeline including ALL building types
    zone_piv_agg_df = zone_df.pivot_table(index  ="zone_id",
                                          columns=["source","year_built_category_agg","building_type"],
                                          values =["residential_units", "building_sqft", "residential_sqft", "non_residential_sqft"],
                                          aggfunc=numpy.sum)
    logger.info("zone_piv_agg_df.head():\n{}".format(zone_piv_agg_df.head()))
    zone_piv_agg_df.reset_index(inplace=True)

    logger.debug("zone_piv_agg_df for zone_id=1: \n{}".format(zone_piv_agg_df.loc[zone_piv_agg_df.zone_id==1].squeeze()))

    # convert column names from tuples
    new_cols = []
    for col in zone_piv_agg_df.columns.values:
        if col[1] == '':  # ('zone_id', '', '', '')
            new_cols.append(col[0])
        else: # ('building_sqft', 'buildings', '0000-2000', 'HM')
            new_cols.append(col[1]+" "+col[2]+" "+col[3]+" "+col[0])
    zone_piv_agg_df.columns = new_cols
    logger.debug("zone_piv_agg_df.head():\n{}".format(zone_piv_agg_df.head()))
    logger.debug("zone_piv_agg_df.dtypes:\n{}".format(zone_piv_agg_df.dtypes))
    logger.debug("zone_piv_agg_df.sum():\n{}".format(zone_piv_agg_df.sum()))

    # merge zone_piv_df and zone_piv_agg_df
    zone_piv_df = pandas.merge(left=zone_piv_df, right=zone_piv_agg_df, left_on="zone_id", right_on="zone_id", how="outer")

    # will create 4 datasets
    KEEP_COLUMNS_BY_DATASET = {
        "base_res": ["zone_id","source",
            "buildings 0000-2000 DM residential_units",
            "buildings 0000-2000 HS residential_units",
            "buildings 0000-2000 HT residential_units",
            "buildings 0000-2000 HM residential_units",
            "buildings 0000-2000 MR residential_units",
            "buildings 0000-2000 all residential_units",

            "buildings 2001-2010 DM residential_units",
            "buildings 2001-2010 HS residential_units",
            "buildings 2001-2010 HT residential_units",
            "buildings 2001-2010 HM residential_units",
            "buildings 2001-2010 MR residential_units",
            "buildings 2001-2010 all residential_units",

            "buildings 2011-2015 DM residential_units",
            "buildings 2011-2015 HS residential_units",
            "buildings 2011-2015 HT residential_units",
            "buildings 2011-2015 HM residential_units",
            "buildings 2011-2015 MR residential_units",
            "buildings 2011-2015 all residential_units",

            "buildings 0000-2015 DM residential_units",
            "buildings 0000-2015 HS residential_units",
            "buildings 0000-2015 HT residential_units",
            "buildings 0000-2015 HM residential_units",
            "buildings 0000-2015 MR residential_units",
            # 2015 HU count
            "buildings 0000-2015 all residential_units",
        ],
        "base_nonres": ["zone_id","source",
            "buildings 0000-2000 all non_residential_sqft",
            "buildings 2001-2010 all non_residential_sqft",
            "buildings 2011-2015 all non_residential_sqft",

            # 2015 Commercial Square Feet
            "buildings 0000-2015 AL non_residential_sqft",
            "buildings 0000-2015 CM non_residential_sqft",
            "buildings 0000-2015 DM non_residential_sqft",
            "buildings 0000-2015 FP non_residential_sqft",
            "buildings 0000-2015 GV non_residential_sqft",
            "buildings 0000-2015 HM non_residential_sqft",
            "buildings 0000-2015 HO non_residential_sqft",
            "buildings 0000-2015 HP non_residential_sqft",
            "buildings 0000-2015 HS non_residential_sqft",
            "buildings 0000-2015 HT non_residential_sqft",
            "buildings 0000-2015 IH non_residential_sqft",
            "buildings 0000-2015 IL non_residential_sqft",
            "buildings 0000-2015 IN non_residential_sqft",
            "buildings 0000-2015 IW non_residential_sqft",
            "buildings 0000-2015 LR non_residential_sqft",
            "buildings 0000-2015 ME non_residential_sqft",
            "buildings 0000-2015 MH non_residential_sqft",
            "buildings 0000-2015 MR non_residential_sqft",
            "buildings 0000-2015 MT non_residential_sqft",
            "buildings 0000-2015 OF non_residential_sqft",
            "buildings 0000-2015 OT non_residential_sqft",
            "buildings 0000-2015 PA non_residential_sqft",
            "buildings 0000-2015 PG non_residential_sqft",
            "buildings 0000-2015 RB non_residential_sqft",
            "buildings 0000-2015 RF non_residential_sqft",
            "buildings 0000-2015 RS non_residential_sqft",
            "buildings 0000-2015 SC non_residential_sqft",
            "buildings 0000-2015 SR non_residential_sqft",
            "buildings 0000-2015 UN non_residential_sqft",
            "buildings 0000-2015 VA non_residential_sqft",
            "buildings 0000-2015 VP non_residential_sqft",

            "buildings 0000-2015 all non_residential_sqft",

            "buildings 0000-2000 all building_sqft",
            "buildings 2001-2010 all building_sqft",
            "buildings 2011-2015 all building_sqft",
            "buildings 0000-2015 all building_sqft",
        ],

        "pipe_res": ["zone_id","source",
            # residential units built from 2016 on
            "pipeline 2016-2020 AL residential_units",
            "pipeline 2016-2020 DM residential_units",
            "pipeline 2016-2020 HS residential_units",
            "pipeline 2016-2020 HT residential_units",
            "pipeline 2016-2020 HM residential_units",
            "pipeline 2016-2020 ME residential_units",
            "pipeline 2016-2020 MR residential_units",
            "pipeline 2016-2020 all residential_units",
    
            "pipeline 2021-2030 AL residential_units",
            "pipeline 2021-2030 DM residential_units",
            "pipeline 2021-2030 HS residential_units",
            "pipeline 2021-2030 HT residential_units",
            "pipeline 2021-2030 HM residential_units",
            "pipeline 2021-2030 ME residential_units",
            "pipeline 2021-2030 MR residential_units",
            "pipeline 2021-2030 all residential_units",
    
            "pipeline 2031-2050 AL residential_units",
            "pipeline 2031-2050 DM residential_units",
            "pipeline 2031-2050 HS residential_units",
            "pipeline 2031-2050 HT residential_units",
            "pipeline 2031-2050 HM residential_units",
            "pipeline 2031-2050 ME residential_units",
            "pipeline 2031-2050 MR residential_units",
            "pipeline 2031-2050 all residential_units",
    
            "pipeline 2016-2050 AL residential_units",
            "pipeline 2016-2050 DM residential_units",
            "pipeline 2016-2050 HS residential_units",
            "pipeline 2016-2050 HT residential_units",
            "pipeline 2016-2050 HM residential_units",
            "pipeline 2016-2050 ME residential_units",
            "pipeline 2016-2050 MR residential_units",
            "pipeline 2016-2050 all residential_units",
        ],

        "pipe_nonres": ["zone_id","source",
            # commercial Square Feet Built From 2016
            "pipeline 2016-2020 all non_residential_sqft",
            "pipeline 2021-2030 all non_residential_sqft",
            "pipeline 2031-2050 all non_residential_sqft",

            "pipeline 2016-2050 AL non_residential_sqft",
            "pipeline 2016-2050 CM non_residential_sqft",
            "pipeline 2016-2050 DM non_residential_sqft",
            "pipeline 2016-2050 FP non_residential_sqft",
            "pipeline 2016-2050 GV non_residential_sqft",
            "pipeline 2016-2050 HM non_residential_sqft",
            "pipeline 2016-2050 HO non_residential_sqft",
            "pipeline 2016-2050 HP non_residential_sqft",
            "pipeline 2016-2050 HS non_residential_sqft",
            "pipeline 2016-2050 HT non_residential_sqft",
            "pipeline 2016-2050 IH non_residential_sqft",
            "pipeline 2016-2050 IL non_residential_sqft",
            "pipeline 2016-2050 IN non_residential_sqft",
            "pipeline 2016-2050 IW non_residential_sqft",
            "pipeline 2016-2050 LR non_residential_sqft",
            "pipeline 2016-2050 ME non_residential_sqft",
            "pipeline 2016-2050 MH non_residential_sqft",
            "pipeline 2016-2050 MR non_residential_sqft",
            "pipeline 2016-2050 MT non_residential_sqft",
            "pipeline 2016-2050 OF non_residential_sqft",
            "pipeline 2016-2050 OT non_residential_sqft",
            "pipeline 2016-2050 PA non_residential_sqft",
            "pipeline 2016-2050 PG non_residential_sqft",
            "pipeline 2016-2050 RB non_residential_sqft",
            "pipeline 2016-2050 RF non_residential_sqft",
            "pipeline 2016-2050 RS non_residential_sqft",
            "pipeline 2016-2050 SC non_residential_sqft",
            "pipeline 2016-2050 SR non_residential_sqft",
            "pipeline 2016-2050 UN non_residential_sqft",
            "pipeline 2016-2050 VA non_residential_sqft",
            "pipeline 2016-2050 VP non_residential_sqft",

            "pipeline 2016-2050 all non_residential_sqft",
        ]
    }

    zone_datasets = {}

    for dataset in KEEP_COLUMNS_BY_DATASET.keys():
        logger.info("Creating dataset for {}".format(dataset))

        keep_columns = KEEP_COLUMNS_BY_DATASET[dataset].copy()

        # but only if they exist
        keep_columns_present = []
        for col in keep_columns: 
            if col in list(zone_piv_df.columns.values): keep_columns_present.append(col)
        zone_dataset_piv_df = zone_piv_df[keep_columns_present]

        # fill na with zero
        zone_dataset_piv_df.fillna(value=0, inplace=True)
    
        logger.info("zone_dataset_piv_df.dtypes:\n{}".format(zone_dataset_piv_df.dtypes))
    
        # add parcel acres
        zone_dataset_piv_df = pandas.merge(left=zone_dataset_piv_df, right=parcels_zone_df, how="outer")

        # and employment, if relevant
        if dataset == "base_nonres":
            zone_dataset_piv_df = pandas.merge(left=zone_dataset_piv_df, right=tm_lu_df, how="outer")
            # and 2015 Employee Density
            zone_dataset_piv_df["Employee Density 2015"] = zone_dataset_piv_df["TOTEMP"]/zone_dataset_piv_df["parcel_acres"]
            zone_dataset_piv_df.loc[ zone_dataset_piv_df["parcel_acres"] == 0, "Employee Density 2015" ] = 0.0

            # 2015 Commercial Square Feet per Employee
            zone_dataset_piv_df["Commercial Square Feet per Employee 2015"] = \
                zone_dataset_piv_df["buildings 0000-2015 all non_residential_sqft"]/zone_dataset_piv_df["TOTEMP"]
            zone_dataset_piv_df.loc[ zone_dataset_piv_df["TOTEMP"] == 0, "Commercial Square Feet per Employee 2015"] = 0.0
    
        # and 2015 HU Density, if relevant
        if dataset == "base_res":
            zone_dataset_piv_df["HU Density 2015"] = zone_dataset_piv_df["buildings 0000-2015 all residential_units"]/zone_dataset_piv_df["parcel_acres"]
            zone_dataset_piv_df.loc[ zone_dataset_piv_df["parcel_acres"] == 0, "HU Density 2015" ] = 0.0

        # zone pivot: add county/superdistrict
        zone_dataset_piv_df = pandas.merge(left=zone_dataset_piv_df, right=taz_sd_county_df, how="outer")
        logger.info("zone_dataset_piv_df.head():\n{}".format(zone_dataset_piv_df.head()))
    
        # write zone_dataset_piv_df
        zone_dataset_piv_file = os.path.join(OUTPUT_DIR, "{}.csv".format(dataset))
        zone_dataset_piv_df.to_csv(zone_dataset_piv_file, index=False)
        logger.info("Wrote {}".format(zone_dataset_piv_file))

        # keep it for arcpy
        zone_datasets[dataset] = zone_dataset_piv_df
    
    # for tableau, let's not pivot, and let's not keep the all btypes
    zone_df = pandas.concat([buildings_zone_btype_df, pipeline_zone_btype_df], axis="index")
    
    # zone: add county/superdistrict
    zone_df = pandas.merge(left=zone_df, right=taz_sd_county_df, how="outer")
    logger.info("zone_df.head():\n{}".format(zone_df.head()))
    
    # write zone_df
    zone_file = os.path.join(OUTPUT_DIR, "urbansim_input_zonedata.csv")
    zone_df.to_csv(zone_file, index=False)
    logger.info("Wrote {}".format(zone_file))

    logger.info("importing arcpy....")
    import arcpy
    arcpy.env.workspace  = WORKSPACE_GDB

    # create metadata
    new_metadata         = arcpy.metadata.Metadata()
    new_metadata.title   = "UrbanSim input"
    new_metadata.summary = "Data derived from UrbanSim Basemap and Development Pipeline for review"
    new_metadata.description = \
        "Basemap source: {}\n".format(URBANSIM_BASEMAP_FILE) + \
        "Pipeline source: {}\n".format(URBANSIM_PIPELINE_FILE) + \
        "Employment source: {}\n".format(EMPLOYMENT_FILE)
    logger.info("Metadata description: {}".format(new_metadata.description))
    new_metadata.credits = "create_tazdata_devpipeline_map.py"

    field_re = re.compile("(pipeline|buildings)_(\S\S\S\S_\S\S\S\S)_([a-zA-Z]+)_(\S+)$")
    for dataset in zone_datasets.keys():
        logger.info("Processing datasset {}".format(dataset))

        # bring in binary of dataset since arcpy mangles csv datatypes
        dataset_table = "{}".format(dataset)
        try:     arcpy.Delete_management(dataset_table)
        except:  pass

        logger.info("Converting dataset to arcpy table {}".format(dataset_table))

        zone_piv_nparr = numpy.array(numpy.rec.fromrecords(zone_datasets[dataset].values))
        zone_piv_nparr.dtype.names = tuple(zone_datasets[dataset].dtypes.index.tolist())
        arcpy.da.NumPyArrayToTable(zone_piv_nparr, os.path.join(WORKSPACE_GDB, dataset_table))

        # create join layer with tazdata and zone_file
        logger.info("Joining {} with {}".format(TAZ_SHPFILE, dataset_table))
        dataset_joined = arcpy.AddJoin_management(TAZ_SHPFILE, "TAZ1454", 
                                                  os.path.join(WORKSPACE_GDB, dataset_table), "zone_id")

        # save it as a feature class -- delete one if it already exists first
        dataset_fc = "{}_fc".format(dataset)
        try:    arcpy.Delete_management(dataset_fc)
        except: pass

        logger.info("Saving it as {}".format(dataset_fc))
        arcpy.CopyFeatures_management(dataset_joined, dataset_fc)

        # set aliases
        ALIASES = {
            # https://github.com/BayAreaMetro/modeling-website/wiki/TazData
            "TOTEMP" : "Total employment",
            "RETEMPN": "Retail employment",
            "FPSEMPN": "Financial and prof employment",
            "HEREMPN": "Health edu and rec employment",
            "AGREMPN": "Ag and natural res employment",
            "MWTEMPN": "Manuf wholesale and transp employment",
            "OTHEMPN": "Other employment"
        }
        fieldList = arcpy.ListFields(dataset_fc)
        for field in fieldList:
            logger.debug("field: [{}]".format(field.name))
            if field.name.startswith("{}_".format(dataset_table)):
                postfix = field.name[len(dataset_table)+1:]
                logger.debug("postfix: [{}]".format(postfix))

                if postfix in ALIASES.keys():
                    arcpy.AlterField_management(dataset_fc, field.name, new_field_alias=ALIASES[postfix])
                else:
                    match = field_re.match(postfix)
                    if match:
                        logger.debug("match: {} {} {} {}".format(match.group(1), match.group(2), match.group(3), match.group(4)))
                        new_alias = ""
                        if match.group(4) == "residential_units":
                            new_alias = "HU "
                        elif match.group(4) == "non_residential_sqft":
                            new_alias = "nonres sqft "
                        elif match.group(4) == "building_sqft":
                            new_alias = "bldg sqft "

                        new_alias += match.group(2) + " " # year range
                        new_alias += BUILDING_TYPE_TO_DESC[match.group(3)] # building type
                        arcpy.AlterField_management(dataset_fc, field.name, new_field_alias=new_alias)


        # set metadata
        logger.info("Setting featureclass metadata")

        # aprx = arcpy.mp.ArcGISProject(ARCGIS_PROJECT)
        # for m in aprx.listMaps():
        #     logger.debug("Map: {}".format(m.name))
        # for lyt in aprx.listLayouts():
        #     logger.debug("Layout: {}".format(lyt.name))
        # aprx.save()

    
        dataset_fc_metadata = arcpy.metadata.Metadata(dataset_fc)
        logger.debug("feature class metadata isReadOnly? {}".format(dataset_fc_metadata.isReadOnly))
        dataset_fc_metadata.copy(new_metadata)
        dataset_fc_metadata.save()
    

    # copy over pipeline with additional info added from building_types_df
    building_types_table = "building_types"
    try:    arcpy.Delete_management(building_types_table)
    except: pass

    building_types_arr = numpy.array(numpy.rec.fromrecords(building_types_df.values))
    building_types_arr.dtype.names = tuple(building_types_df.dtypes.index.tolist())
    arcpy.da.NumPyArrayToTable(building_types_arr, os.path.join(WORKSPACE_GDB, building_types_table))

    # create join layer with tazdata and zone_file
    logger.info("Joining {} with {}".format(os.path.join(URBANSIM_PIPELINE_GDB, "pipeline"), building_types_table))
    dataset_joined = arcpy.AddJoin_management(os.path.join(URBANSIM_PIPELINE_GDB, "pipeline"), "building_type", 
                                              os.path.join(WORKSPACE_GDB, building_types_table), "building_type_det")
    pipeline_fc = "pipeline"
    try:    arcpy.Delete_management(pipeline_fc)
    except: pass

    logger.info("Saving it as {}".format(pipeline_fc))
    arcpy.CopyFeatures_management(dataset_joined, pipeline_fc)
    
    logger.info("Complete")

"""
=> Save individual layer as web layer

Go to Service URL (ugly webpage that looks like services3.argics.com...)
Click on link View In: ArcGIS.com Map
Add that to the web map and the field aliases will work

Pop-up panel widget for custom web app:
https://community.esri.com/docs/DOC-7355-popup-panel-widget-version-213-102519

Basemap Employment for TAZ {base_nonres_zone_id}

<font size="2">
<table>
<tr><td>Total Employment:</td><td>{base_nonres_TOTEMP}</td></tr>
<tr><td>Retail:</td><td>{base_nonres_RETEMPN}</td></tr>
<tr><td>Financial &amp; prof:</td><td>{base_nonres_FPSEMPN}</td></tr>
<tr><td>Health, educ &amp; rec:</td><td>{base_nonres_HEREMPN}</td></tr>
<tr><td>Ag &amp; natural res.:</td><td>{base_nonres_AGREMPN}</td></tr>
<tr><td>Manuf, wholesale and transp:</td><td>{base_nonres_MWTEMPN}</td></tr>
<tr><td>Other:</td><td>{base_nonres_OTHEMPN}</td></tr><p><span></td></tr>
<tr><td>Parcel acres:</td><td>{base_nonres_parcel_acres}</td></tr>
<tr><td>Employee Density:</td><td>{base_nonres_Employee_Density_2015}</td></tr>
<tr><td>Non-Residential Square Feet:</td><td>{base_nonres_buildings_0000_2015_all_no}</td></tr>
<tr><td>Commercial Square Feet per Employee:</td><td>{base_nonres_Commercial_Square_Feet_per}</td></tr>
</table>
</font>

Charts:
Employment by Industry
Non-Residential Sqft by Year Built

Basemap Housing Units for TAZ {base_res_zone_id}

Charts:
Housing Unit by Building Type
Housing Unit by Year Built


Decode( building_type,
       'HS','single family residential'
       'HT','townhomes',
       'HM','multi family residential',
       'MH','mobile home',
       'SR','single room occupancy',
       'AL','assisted living',
       'DM','dorm or shelter',
       'CM','condo or apt common area',
       'OF','office',
       'GV','gov',
       'HP','hospital',
       'HO','hotel',
       'SC','k12 school',
       'UN','college or university',
       'IL','light industrial',
       'FP','food processing and wineries',
       'IW','warehouse or logistics',
       'IH','heavy industrial',
       'RS','retail general',
       'RB','retail big box or regional',
       'MR','mixed use residential focused',
       'MT','mixed use industrial focused',
       'ME','mixed use employment focused',
       'PA','parking lot',
       'PG','parking garage',
       'VA','vacant',
       'LR','golf course or other low density rec use',
       'VP','vacant permanently such as park or transport net',
       'OT','other or unknown',
       'IN','institutional',
       'RF','retail food or drink',
       'unknown')

"""