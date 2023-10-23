# for arcpy:
# set PATH=C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3
USAGE = """

Creates BASIS vs PBA40 maps comparing DUA, etc by jurisdiction.
Includes information on whether or not the BASIS data was reviewed (for relevant types of data) and
whether or not UrbanSim input is currently configured to use the BASIS data.

"""

import argparse, collections, csv, datetime, os, sys, time, traceback
import arcpy, numpy, pandas, xlrd

import dev_capacity_calculation_module

COUNTY_JURISDICTIONS_CSV = "M:\\Data\\GIS layers\\Jurisdictions\\county_jurisdictions.csv"

if os.getenv("USERNAME")=="lzorn":
    # This was created by joining output of 1_PLU_BOC_data_combine.ipynb with p10
    #
    # e.g. using the command
    #
    # python import_filegdb_layers.py "M:\Data\GIS layers\UrbanSim smelt\2020 03 12\smelt.gdb" p10 PARCEL_ID 
    #   "C:\Users\ywang\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\PBA50\Policies\Base zoning\outputs\2020_10_20_p10_plu_boc_allAttrs.csv"
    #   PARCEL_ID "KEEP_ALL" "M:\Data\GIS layers\UrbanSim_BASIS_zoning\UrbanSim_BASIS_zoning_fb.gdb"
    #
    WORKSPACE_DIR   = "M:\\Data\\GIS layers\\UrbanSim_BASIS_zoning"
    WORKSPACE_GDB   = os.path.join(WORKSPACE_DIR,"UrbanSim_BASIS_zoning_fb.gdb")
    ARCGIS_PROJECTS = [os.path.join(WORKSPACE_DIR,"UrbanSim_BASIS_zoning_intensity_fb.aprx"),
                       os.path.join(WORKSPACE_DIR,"UrbanSim_BASIS_zoning_devType_fb.aprx")]

    # location of BASIS_Local_Jurisdiction_Review_Summary.xlsx (https://mtcdrive.box.com/s/s2w68pnboa3gzq5z228mqbxtdehgdcxd)
    JURIS_REVIEW    = "C:\\Users\\lzorn\\Box\\BASIS Land Use Data Store\\Jurisdiction Review\\BASIS_Local_Jurisdiction_Review_Summary.xlsx"

    PETRALE_GITHUB_DIR = "X:\\petrale"
    # location of current hybrid configuration
    HYBRID_CONFIG_DIR   = os.path.join(PETRALE_GITHUB_DIR, "policies", "plu", "base_zoning", "hybrid_index")

elif os.getenv("USERNAME")=="ywang":

    # This was created by joining output of 1_PLU_BOC_data_combine.ipynb with p10
    #
    # e.g. using the command
    #
    # python import_filegdb_layers.py "M:\Data\GIS layers\UrbanSim smelt\2020 03 12\smelt.gdb" p10 PARCEL_ID 
    #   "C:\Users\ywang\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\PBA50\Policies\Base zoning\outputs\2020_10_20_p10_plu_boc_allAttrs.csv"
    #   PARCEL_ID "KEEP_ALL" "M:\Data\GIS layers\UrbanSim_BASIS_zoning\UrbanSim_BASIS_zoning_fb.gdb"

    WORKSPACE_DIR   = "M:\\Data\\GIS layers\\UrbanSim_BASIS_zoning"
    WORKSPACE_GDB   = os.path.join(WORKSPACE_DIR,"UrbanSim_BASIS_zoning_fb.gdb")
    ARCGIS_PROJECTS = [os.path.join(WORKSPACE_DIR,"UrbanSim_BASIS_zoning_intensity_fb.aprx"),
                       os.path.join(WORKSPACE_DIR,"UrbanSim_BASIS_zoning_devType_fb.aprx")]

    # location of BASIS_Local_Jurisdiction_Review_Summary.xlsx (https://mtcdrive.box.com/s/s2w68pnboa3gzq5z228mqbxtdehgdcxd)
    JURIS_REVIEW    = "C:\\Users\\ywang\\Documents\\Python Scripts\\UrbanSim_BASIS_zoning\\BASIS_Local_Jurisdiction_Review_Summary.xlsx"

    PETRALE_GITHUB_DIR = "C:\\Users\\ywang\\Documents\\GitHub\\petrale"
    # location of current hybrid configuration
    HYBRID_CONFIG_DIR  = os.path.join(PETRALE_GITHUB_DIR, "policies", "plu", "base_zoning", "hybrid_index")


if __name__ == '__main__':
    pandas.options.display.max_rows = 999

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("--debug",         help="If on, saves a copy of the arcgis project with mods.", action='store_true')
    parser.add_argument("--restart_juris", help="Jurisdiction to restart from")
    parser.add_argument("--jurisdiction",  help="Jurisdiction. If none passed, will process all", nargs='+', )
    parser.add_argument("--metric",        help="Metrics type(s). If none passed, will process all", nargs='+',
                                           choices=["DUA","FAR","height","HS","HT","HM","OF","HO","SC","IL","IW","IH","RS","RB","MR","MT","ME"])
    parser.add_argument("--hybrid_config", help="Required arg. Hybrid config file in {}".format(HYBRID_CONFIG_DIR), required=True)
    parser.add_argument("--output_type",   help="Type of map to export", choices=["pdf","png"], default="pdf")
    args = parser.parse_args()

    # read list of jurisdictions
    JURISDICTION_TO_COUNTY = collections.OrderedDict()

    with open(COUNTY_JURISDICTIONS_CSV, mode='r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            JURISDICTION_TO_COUNTY[row['Jurisdiction']] = row['County']
    
    # read jurisdiction review status for BASIS
    juris_review_df = pandas.read_excel(JURIS_REVIEW, sheet_name="Sheet 1", header=1)
    juris_review_df = juris_review_df.loc[ pandas.notnull(juris_review_df.Jurisdiction) ]
    juris_review_df.set_index("Jurisdiction", inplace=True)
    # print(juris_review_df)
    juris_review_dict = juris_review_df.to_dict(orient="index")
    # print(juris_review_dict["Berkeley"])
    # e.g. {
    #  'County': 'Alameda', 
    #  'Check Allowable Building Heights': True, 
    #  'Check Development Pipeline': True,
    #  'Check Floor Area Ratio': True,
    #  'Check Residential Densities': True,
    #  'Check Spheres of Influence': True,
    #  'Check Urban Growth Boundaries': True,
    #  'Check Zoning Codes': True,
    #  'Check Zoning Description': True,
    #  'Zoning Document': True,
    #  'Zoning Map': True,
    #  'Zoning Ordinance Effective Date': True,
    #  'Check Zoning Parcel Map': True,
    #  'Percent Complete': 1
    # }

    # read hybrid configuration 
    hybrid_config_df = pandas.read_csv(os.path.join(HYBRID_CONFIG_DIR, args.hybrid_config))
    # print(hybrid_config_df.head())
    hybrid_config_df.set_index("juris_name", inplace=True)
    hybrid_config_dict = hybrid_config_df.to_dict(orient="index")
    print(hybrid_config_dict["berkeley"])
    # e.g. {
    #  'juris_id': 'berk',
    #  'county': 'ala',
    #  'OF_idx': 0,
    #  'HO_idx': 0,
    #  'SC_idx': 0,
    #  'IL_idx': 0,
    #  'IW_idx': 0, 
    #  'IH_idx': 0,
    #  'RS_idx': 0,
    #  'RB_idx': 0,
    #  'MR_idx': 0,
    #  'MT_idx': 0,
    #  'ME_idx': 0,
    #  'HS_idx': 0,
    #  'HT_idx': 0,
    #  'HM_idx': 1,
    #  'max_dua_idx': 0,
    #  'max_far_idx': 0,
    #  'max_height_idx': 0,
    #  'proportion_adj_dua': 1,
    #  'proportion_adj_far': 1, 
    #  'proportion_adj_height': 1
    # }

    # if jurisdictino passed, remove others and only process that one
    if args.jurisdiction:
        JURISDICTION_TO_COUNTY_arg = {}
        for juris in args.jurisdiction:
            if juris not in JURISDICTION_TO_COUNTY:
                print("Jurisdiction [{}] not found in {}".format(juris, COUNTY_JURISDICTIONS_CSV))
            else:
                JURISDICTION_TO_COUNTY_arg[juris] = JURISDICTION_TO_COUNTY[juris]

        # use that instead
        JURISDICTION_TO_COUNTY = JURISDICTION_TO_COUNTY_arg
    elif args.restart_juris:
        print("Restarting at jurisdiction {}".format(args.restart_juris))
        juris_list = list(JURISDICTION_TO_COUNTY.keys())
        for juris in juris_list:
            if juris == args.restart_juris: break

            del JURISDICTION_TO_COUNTY[juris]

    print("Will process jurisdictions: {}".format(JURISDICTION_TO_COUNTY.keys()))

    # set the workspace
    arcpy.env.workspace = WORKSPACE_GDB
    now_str = datetime.datetime.now().strftime("%Y/%m/%d, %H:%M")

    source_str = \
        "<FNT size=\"7\">Created by " \
        "<ITA>https://github.com/BayAreaMetro/petrale/blob/master/policies/plu/base_zoning/create_jurisdiction_map.py</ITA> on {}. " \
        "Hybrid config: <ITA>https://github.com/BayAreaMetro/petrale/blob/master/policies/plu/base_zoning/hybrid_index/{}</ITA></FNT>".format(
            now_str, args.hybrid_config)

    METRICS_DEF = collections.OrderedDict([
                   # ArcGIS project,                        detail name,                        BASIS jurisdiction col,             hybrid config col
        ('DUA'    ,["UrbanSim_BASIS_zoning_intensity.aprx", 'DUA',                              'Check Residential Densities',     'max_dua_idx'   ]),
        ('FAR'    ,["UrbanSim_BASIS_zoning_intensity.aprx", 'FAR',                              'Check Floor Area Ratio',          'max_far_idx'   ]),
        ('height' ,["UrbanSim_BASIS_zoning_intensity.aprx", 'height',                           'Check Allowable Building Heights','max_height_idx']),
        # residential
        ('HS'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow HS (Single-family Housing)', None,                              'HS_idx'        ]),
        ('HT'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow HT (Row-House Dwelling)',    None,                              'HT_idx'        ]),
        ('HM'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow HM (Multi-family Housing)',  None,                              'HM_idx'        ]),
        ('MR'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow MR (Mixed-use Residential)', None,                              'MR_idx'        ]),
        # non residential
        ('OF'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow OF (Office)',                None,                              'OF_idx'        ]),
        ('HO'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow HO (Hotel)',                 None,                              'HO_idx'        ]),
        ('SC'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow SC (School)',                None,                              'SC_idx'        ]),
        ('IL'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow IL (Light Industrial)',      None,                              'IL_idx'        ]),
        ('IW'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow IW (Warehouse+Logistics)',   None,                              'IW_idx'        ]),
        ('IH'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow IH (Heavy Industrial)',      None,                              'IH_idx'        ]),
        ('RS'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow RS (Retail)',                None,                              'RS_idx'        ]),
        ('RB'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow RB (Big Box Retail)',        None,                              'RB_idx'        ]),
        ('MT'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow MT (Mixed-use Retail)',      None,                              'MT_idx'        ]),
        ('ME'     ,["UrbanSim_BASIS_zoning_devType.aprx",   'Allow ME (Mixed-use Office)',      None,                              'ME_idx'        ]),
    ])

    # these are the metrics we'll process
    if args.metric:
        metric_list = args.metric
    else:
        metric_list = list(METRICS_DEF.keys())
    print("Will process metrics: {}".format(metric_list))

    prev_jurisdiction = "Palo Alto"
    prev_juris_code   = "palo_alto"

    prev_allowdevtype_metric = "HM"

    for jurisdiction in JURISDICTION_TO_COUNTY.keys():

        juris_code = jurisdiction.lower().replace(" ","_").replace(".","")
        print("Creating map for {} ({})".format(jurisdiction, juris_code))

        metric_idx = 0
        while metric_idx < len(metric_list):
            metric = metric_list[metric_idx]

            try:

                print("  Creating map for metric {}".format(metric))
                arc_project      = METRICS_DEF[metric][0]
                metric_name      = METRICS_DEF[metric][1]
                basis_check_col  = METRICS_DEF[metric][2]
                basis_hybrid_col = METRICS_DEF[metric][3]
    
                basis_check_val  = False
                if basis_check_col:
                    if jurisdiction not in juris_review_dict:
                        print("Couldn't find jurisdiction {} in BASIS jurisdiction review {}".format(jurisdiction, JURIS_REVIEW))
                    else:
                        basis_check_val = juris_review_dict[jurisdiction][basis_check_col]
                        print("  BASIS check val for {}: {}".format(basis_check_col, basis_check_val))
                basis_hybrid_val = hybrid_config_dict[juris_code][basis_hybrid_col]
                print("  BASIS hybrid config val for {}: {}".format(basis_hybrid_col, basis_hybrid_val))
    
                # allowed dev type has a generic map so needs subsitution for that as well
                is_devtype      = False
                map_metric      = metric
                map_metric_name = metric_name
                if metric in dev_capacity_calculation_module.ALLOWED_BUILDING_TYPE_CODES:
                    is_devtype      = True
                    map_metric      = prev_allowdevtype_metric
                    map_metric_name = METRICS_DEF[prev_allowdevtype_metric][1]
                print("   map_metric:[{}]  map_metric_name:[{}]".format(map_metric, map_metric_name))
    
                # start fresh
                aprx       = arcpy.mp.ArcGISProject(arc_project)
                layouts    = aprx.listLayouts("Layout_{}".format(map_metric))
                maps       = aprx.listMaps()
                juris_lyr  = {} # key: "BASIS" or "PBA40"
    
                assert(len(layouts)==1)
    
                for my_map in maps:
                    if my_map.name.endswith(map_metric) or my_map.name.endswith(map_metric_name):
                        # process this one
                        print("  Processing map {}".format(my_map.name))
                    else:
                        print("  Skipping map {}".format(my_map.name))
                        continue
    
                    for layer in my_map.listLayers():
                        if not layer.isFeatureLayer: continue
                        print("    Processing layer {}".format(layer.name))
                        print("      Definition query: {}".format(layer.definitionQuery))
                        # modify to current jurisdiction
                        layer.definitionQuery = layer.definitionQuery.replace(prev_jurisdiction, jurisdiction)
                        layer.definitionQuery = layer.definitionQuery.replace(prev_juris_code,   juris_code)
                        # modify to current devtype
                        if is_devtype:
                            layer.definitionQuery = layer.definitionQuery.replace(prev_allowdevtype_metric, metric)
                            layer.name            = layer.name.replace(prev_allowdevtype_metric, metric)
    
                        print("      => Definition query: {}".format(layer.definitionQuery))
    
                        # for devtype, may need to change variable used which means updating symbology
                        if is_devtype:
                            print("      Symbology: {}".format(layer.symbology))
                            if hasattr(layer.symbology, 'renderer') and layer.symbology.renderer.type=='UniqueValueRenderer':
                                
                                fields     = layer.symbology.renderer.fields
                                new_fields = [field.replace(prev_allowdevtype_metric, metric) for field in fields]
    
                                # following example here: https://pro.arcgis.com/en/pro-app/arcpy/mapping/uniquevaluerenderer-class.htm
                                sym = layer.symbology
                                sym.updateRenderer('UniqueValueRenderer')
                                sym.renderer.fields = new_fields
                                print("      Symbology.renderer.fields: {} => {}".format(fields, new_fields))
                                for grp in sym.renderer.groups:
                                    for itm in grp.items:
                                        if itm.values == [['0']]:
                                            itm.label        = 'Not Allowed'
                                            itm.symbol.color = {'RGB': [199, 215, 158, 100]}  # light green
                                            itm.symbol.size  = 0.0  # outline width => no outline
                                        elif itm.values == [['1']]:
                                            itm.label        = 'Allowed'
                                            itm.symbol.color = {'RGB': [230, 152, 0, 100]}   # orange
                                            itm.symbol.size  = 0.0  # outline width => no outline
                                        elif itm.values == [['<Null>']]:
                                            itm.label        = 'Missing'
                                            itm.symbol.color = {'RGB': [0, 77, 168, 100]}  # blue
                                            itm.symbol.size  = 0.0  # outline width => no outline
                                        else:
                                            print("      Don't recognize itm.values: {}".format(itm.values))
                                layer.symbology = sym
    
    
                        # save this for extent
                        if layer.name == "Jurisdictions - primary":
                            juris_lyr[my_map.name] = layer
                            print("      saving juris_lyr[{}]".format(my_map.name))
    
                layout = layouts[0]
    
    
                print("  Processing layout {}".format(layout.name))
                for element in layout.listElements():
                    print("    Processing element {}: {}".format(element.name, element))
    
                    if element.name == "Source":
                        element.text = source_str
                    if element.name == "Jurisdiction":
                        element.text = jurisdiction
    
                    if element.name == "juris_review_false":
                        element.visible = not basis_check_val   # visible if basis_check_val==False
                    if element.name == "juris_review_true":
                        element.visible =  basis_check_val      # visible if basis_check_val==True
    
                    if element.name == "arrow_basis":
                        element.visible = basis_hybrid_val      # visible if basis_hybrid_val==True
                    if element.name == "input_basis":
                        element.visible = basis_hybrid_val      # visible if basis_hybrid_val==True
    
                    if element.name == "arrow_pba40":
                        element.visible = not basis_hybrid_val  # visible if basis_hybrid_val==False
                    if element.name == "input_pba40":
                        element.visible = not basis_hybrid_val  # visible if basis_hybrid_val==False
    
                    if is_devtype and element.name == "BASIS Label":
                        element.text = "BASIS {}".format(metric_name)
                    if is_devtype and element.name == "PBA40 Label":
                        element.text = "PBA40 {}".format(metric_name)
    
                    # zoom to the jurisdiction
                    if element.name.find("Map Frame") >= 0:
                        if element.name.endswith("BASIS"):
                            map_type = "BASIS_"+map_metric
                        else:
                            map_type = "PBA40_"+map_metric
    
                        # get the jurisdiction layer extent
                        layer_extent = element.getLayerExtent(juris_lyr[map_type])
                        # apply extent to mapframe camera
                        element.camera.setExtent(layer_extent)
    
                if args.output_type == "pdf":
                    juris_pdf = "{}_{}.pdf".format(juris_code, metric_name)
                    layout.exportToPDF(juris_pdf)
                    print("  Wrote {}".format(juris_pdf))
                elif args.output_type == "png":
                    juris_png = "{}_{}.png".format(juris_code, metric_name)
                    layout.exportToPNG(juris_png, resolution=300)    
                    print("  Wrote {}".format(juris_png))
    
    
                # if instructed, save a copy of the arcgis project
                if args.debug:
                    copy_filename = arc_project.replace(".aprx","_{}_{}.aprx".format(juris_code,metric))
                    aprx.saveACopy(copy_filename)
                    print("DEBUG: saved a copy of project to {}".format(copy_filename))

                # successfully completed this metric
                metric_idx += 1
            except:

                print("======= Unexpected error: {}".format(sys.exc_info()))
                exc_type, exc_value, exc_tb = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_tb)
                # print("======= Retrying {} {}".format(jurisdiction, metric))
                # print("")
                # print("")
                # continue

                # This exception seems to occur sporadically running this script:
                # Traceback (most recent call last):
                #   File "X:\petrale\policies\plu\base_zoning\create_jurisdiction_map.py", line 273, in <module>
                #     sym.renderer.fields = new_fields
                #   File "C:\Program Files\ArcGIS\Pro\Resources\ArcPy\arcpy\arcobjects\_base.py", line 109, in _set
                #     return setattr(self._arc_object, attr_name, cval(val))
                # RuntimeError: Invalid set of Fileds : ['p10_plu_boc_allAttrs_IW_basis']
                
                # note: I tried to retry using the continue line above, but in practice it ended up looping and being
                # unable to resolve the issue.  So just quit
                sys.exit(2)

        # done with jurisdiction
        print("")


