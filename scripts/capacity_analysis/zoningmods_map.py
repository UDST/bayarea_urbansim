USAGE = """

Create zoning_mods-level spatial data in three steps:
1) join p10 parcel data with parcel-level zoningmods_attributes.csv
2) dissolve the joined layer by zoning_mods geography (fbpzoningmodcat for Final Blueprint)
3) join the dissolved layer with zoning_mods.csv which contains upzoning information

Run the script in the desired folder for the output .gdb

Use ArcGIS python for arcpy:
set PATH=C:\\Program Files\\ArcGIS\\Pro\\bin\\Python\\envs\\arcgispro-py3
   or set PATH=C:\\Users\\ywang\\AppData\\Local\\Programs\\ArcGIS\\Pro\\bin\\Python\\envs\\arcgispro-py3

"""

# example run:
# set BAUS_DIR=%USERPROFILE%\Documents\bayarea_urbansim
# set FBP_DIR=%USERPROFILE%\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\PBA50\Current PBA50 Large General Input Data
# python zoningmods_map.py -folder .
#                          -input_gdb "M:\Data\GIS layers\UrbanSim smelt\2020 03 12\smelt.gdb"
#                          -p10_layer p10
#                          -parcels_geography "%FBP_DIR%\2020_09_21_parcels_geography.csv" 
#                          -zmods_csv "%BAUS_DIR%\data\zoning_mods_24.csv"
#                          -zmodcat_col fbpzoningmodcat
#                          -join_field PARCEL_ID 
#                          -join_type KEEP_ALL
#                          -output_gdb "FinalBlueprint_ZoningMods_20201002.gdb" 
#
# Draft Blueprint release: https://github.com/BayAreaMetro/bayarea_urbansim/releases/tag/v1.9  (July 31, 2020)
# commit: 7183846409013a6175e613f11f032513e7dbe51d
#
# Note: though the v1.9 datasources.py (https://github.com/BayAreaMetro/bayarea_urbansim/blob/7183846409013a6175e613f11f032513e7dbe51d/baus/datasources.py#L492) 
# says parcels_geography is 2020_07_10_parcels_geography.csv
# But the "v1.7.1- FINAL DRAFT BLUEPRINT" run98 was on June 22, 2020
# So assuming the parcels_geography used was 2020_04_17_parcels_geography.csv
# 
# set BAUS_DIR=%USERPROFILE%\Documents\bayarea_urbansim
# set DBP_DIR=%USERPROFILE%\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\PBA50\Draft Blueprint Large Input Data
# python zoningmods_map.py -folder .
#                          -input_gdb "M:\Data\GIS layers\UrbanSim smelt\2020 03 12\smelt.gdb"
#                          -p10_layer p10
#                          -parcels_geography "%DBP_DIR%\2020_04_17_parcels_geography.csv"
#                          -zmods_csv "%BAUS_DIR%\data\zoning_mods_23.csv"
#                          -zmodcat_col pba50zoningmodcat
#                          -join_field PARCEL_ID
#                          -join_type KEEP_ALL
#                          -output_gdb DraftBlueprint_ZoningMods.gdb

import argparse, os, sys, time
import arcpy, pandas


if __name__ == '__main__':

    start = time.time()

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("-folder",         metavar="folder",      help="Working folder")
    parser.add_argument("-input_gdb",      metavar="input.gdb",   help="Input geodatabase")
    parser.add_argument("-output_gdb",     metavar="output.gdb",  help="Output geodatabase")
    parser.add_argument("-p10_layer",      metavar="p10_layer",   help="p10 parcel layer")
    parser.add_argument("-parcels_geography", help="Parcels geography layer (maps parcels to zoning mod category)")
    parser.add_argument("-zmods_csv",      metavar="zmods.csv",   help="Zoning mods definition for zoning mod categories")
    parser.add_argument("-zmodcat_col",    help="Zoning mod category column. e.g. pba50zoningmodcat or fbpzoningmodcat")
    parser.add_argument("-join_field",     metavar="join_field",  help="Join field for parcel-zmods join")
    parser.add_argument("-join_type",      choices=["KEEP_ALL","KEEP_COMMON"], default="KEEP_ALL", 
                        help="Outer join vs inner join.  Default is KEEP_ALL, or outer")

    args = parser.parse_args()
    args.folder = os.path.abspath(args.folder)
    print(" {:18}: {}".format("folder",         args.folder))
    print(" {:18}: {}".format("input_gdb",      args.input_gdb))
    print(" {:18}: {}".format("output_gdb",     args.output_gdb))
    print(" {:18}: {}".format("p10_layer",      args.p10_layer))
    print(" {:18}: {}".format("parcels_geography", args.parcels_geography))
    print(" {:18}: {}".format("zmods_csv",      args.zmods_csv))
    print(" {:18}: {}".format("zmodcat_col",    args.zmodcat_col))
    print(" {:18}: {}".format("join_field",     args.join_field))
    print(" {:18}: {}".format("join_type",      args.join_type))


    # create output_gdb if not exists already
    if not os.path.exists(os.path.join(args.folder,args.output_gdb)):
        (head,tail) = os.path.split(os.path.join(args.folder,args.output_gdb))
        print("head: {} tail: {}".format(head, tail))
        if head=="": head="."
        arcpy.CreateFileGDB_management(head, tail)
        print("Created {}".format(os.path.join(args.folder,args.output_gdb)))

    arcpy.env.workspace = os.path.join(args.folder,args.output_gdb)

    ########## Join zmods_attr layer to p10 parcel layer ##########

    # read zmods_attr file
    zmod_cols = [args.join_field, args.zmodcat_col]
    zmod_attr = pandas.read_csv(os.path.join(args.folder, args.parcels_geography), 
                                usecols = zmod_cols)
    print("Read {} records from {}\nwith {} unique {} and {} unique {}".format(
          len(zmod_attr), os.path.join(args.folder, args.parcels_geography),
          len(zmod_attr[args.join_field].unique()), args.join_field,
          len(zmod_attr[args.zmodcat_col].unique()), args.zmodcat_col))
    # copy the table to output_gdb
    print("Copy {} to {}".format(args.parcels_geography, 
                                 os.path.join(args.folder, args.output_gdb)))
    
    # Note: can't rename after args.parcels_geography because ArcGIS errors for some table names (e.g. those starting with a number)
    zmod_attr_table = "parcel_geography"
    print("zmod_attr_table={}".format(zmod_attr_table))

    # delete table if there's already one there by that name
    if arcpy.Exists(zmod_attr_table):
        arcpy.Delete_management(zmod_attr_table)
        print("Found {} -- deleting".format(zmod_attr_table))

    zmod_attr_values = numpy.array(numpy.rec.fromrecords(zmod_attr.values))
    zmod_attr_values.dtype.names = tuple(zmod_attr.dtypes.index.tolist())
    zmod_attr_table_path = os.path.join(args.folder, args.output_gdb, zmod_attr_table)
    arcpy.da.NumPyArrayToTable(zmod_attr_values, zmod_attr_table_path)
    print("Created {} with {} records".format(zmod_attr_table_path,
                                              arcpy.GetCount_management(zmod_attr_table_path)))

    # target layer
    p10 = os.path.join(args.folder, args.input_gdb, args.p10_layer)
    print("Target layer: {}".format(p10))

    # copy the layer to output_gdb
    print("Copy {} to {}".format(p10, 
                                 os.path.join(args.folder, args.output_gdb)))

    # delete the layer if it already exists in the output gdb
    if arcpy.Exists(args.p10_layer):
        arcpy.Delete_management(args.p10_layer)
        print("Found {} -- deleting".format(args.p10_layer))

    # copy the input to output_gdb with the same name
    arcpy.CopyFeatures_management(os.path.join(args.folder, args.input_gdb, args.p10_layer),
                                  os.path.join(args.folder, args.output_gdb, args.p10_layer))

    # join table to the target layer
    print("Joining {} with {}".format(os.path.join(args.folder, args.output_gdb, args.p10_layer),
                                      os.path.join(args.folder, args.output_gdb, zmod_attr_table)))

    p_zmod_attr_join = arcpy.AddJoin_management(args.p10_layer, args.join_field,
                                                zmod_attr_table, args.join_field,
                                                join_type=args.join_type)

    p_zmod_attr_joined = "p10_zmod_attr_joined"

    # delete the layer if it already exists in the output gdb
    if arcpy.Exists(p_zmod_attr_joined):
        arcpy.Delete_management(p_zmod_attr_joined)
        print("Found {} -- deleting".format(p_zmod_attr_joined))

    # save it
    arcpy.CopyFeatures_management(p_zmod_attr_join, os.path.join(args.folder, args.output_gdb, p_zmod_attr_joined))
    print("Completed creation of {}".format(os.path.join(args.folder, args.output_gdb, p_zmod_attr_joined)))
    field_names = [f.name for f in arcpy.ListFields(p_zmod_attr_joined)]
    print("{} has the following fields: {}".format(p_zmod_attr_joined,
                                                   field_names))

    ########## Dissolve the joint parcel-zmods layer by zoningmod category ##########

    print("Dissolve {} on field: {}".format(p_zmod_attr_joined,
                                            [zmod_attr_table+'_'+args.zmodcat_col]))
    #p_zmod_dissolved = 'p10_zmods_dissolved_{}'.format(zmod_attr_version)
    p_zmod_dissolved = 'p10_zmods_dissolved'

    # delete the layer if it already exists in the output gdb
    if arcpy.Exists(p_zmod_dissolved):
        arcpy.Delete_management(p_zmod_dissolved)
        print("Found {} -- deleting".format(p_zmod_dissolved))

    arcpy.Dissolve_management(p_zmod_attr_joined,
                              os.path.join(args.folder, args.output_gdb, p_zmod_dissolved),
                              [zmod_attr_table+'_'+args.zmodcat_col], "")

    field_names = [f.name for f in arcpy.ListFields(p_zmod_dissolved)]
    print("Dissolve completed; {} has {} records and the following fields \n{}".format(
            p_zmod_dissolved,
            arcpy.GetCount_management(p_zmod_dissolved),
            field_names))

    ########## Join the dissolved parcels to zoning_mods ##########
    
    # read zoning_mods file
    zmods = pandas.read_csv(args.zmods_csv)

    print("Read {} records from {}, with {} unique {} and the following fields: \n{}".format(
            len(zmods), args.zmods_csv,       
            len(zmods[args.zmodcat_col].unique()), args.zmodcat_col,
            list(zmods)))

    # copy the table to output_gdb
    print("Copy {} to {}".format(args.zmods_csv, 
                                 os.path.join(args.folder, args.output_gdb)))
    zmods_table = os.path.split(args.zmods_csv)[1]  # remove directory if full path
    zmods_table = os.path.splitext(zmods_table)[0]  # remove file extension

    # delete table if there's already one there by that name
    if arcpy.Exists(zmods_table):
        arcpy.Delete_management(zmods_table)
        print("Found {} -- deleting".format(zmods_table))

    zmods_values = numpy.array(numpy.rec.fromrecords(zmods.values))
    zmods_values.dtype.names = tuple(zmods.dtypes.index.tolist())
    arcpy.da.NumPyArrayToTable(zmods_values, os.path.join(args.folder, args.output_gdb, zmods_table))
    print("Created {}".format(os.path.join(args.folder, args.output_gdb, zmods_table)))

    # join table to the dissolved layer
    print("Joining {} with {}".format(os.path.join(args.folder, args.output_gdb, p_zmod_dissolved),
                                      os.path.join(args.folder, args.output_gdb, zmods_table)))    

    p_zmods_join = arcpy.AddJoin_management(p_zmod_dissolved, zmod_attr_table+'_'+args.zmodcat_col,
                                            zmods_table, args.zmodcat_col,
                                            join_type=args.join_type)

    zmods_version = args.zmods_csv.split('.')[0].split('_')[-1]
    p_zmods_joined = "p10_zoningmods_{}".format(zmods_version)

    # delete the layer if it already exists in the output gdb
    if arcpy.Exists(p_zmods_joined):
        arcpy.Delete_management(p_zmods_joined)
        print("Found {} -- deleting".format(p_zmods_joined))

    # save it
    arcpy.CopyFeatures_management(p_zmods_join, os.path.join(args.folder, args.output_gdb, p_zmods_joined))
    print("Completed creation of {}".format(os.path.join(args.folder, args.output_gdb, p_zmods_joined)))

    print("{} has {} records".format(p_zmods_joined, 
                                     arcpy.GetCount_management(p_zmods_joined)[0]))

    print("Script took {0:0.1f} minutes".format((time.time()-start)/60.0))