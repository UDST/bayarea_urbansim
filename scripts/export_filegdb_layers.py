USAGE = """
  Pass in a file geodatabase and layer name and this will export into the current working directory:

  1) a shapefile if the layer is a feature class
  2) a dbf if the layer is a table

  To see a list of the feature classes and/or tables, pass the geodatabase name only.

Use ArcGIS python for arcpy
set PATH=C:\\Program Files\\ArcGIS\\Pro\\bin\\Python\\envs\\arcgispro-py3

"""

import argparse, os, sys, time
import arcpy

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("geodatabase",  metavar="geodatabase.gdb", help="File geodatabase with layer export")
    parser.add_argument("--layer", help="Layer to export")
    parser.add_argument("--format", choices=["csv","dbf","shp","geojson"])

    args = parser.parse_args()

    arcpy.env.workspace = args.geodatabase

    # a dictionary of fields with required data type
    # add to this dictionary as needed
    field_types = {'parcel_id': 'LONG', 
                   'zone_id':   'SHORT',
                   'geom_id_s': 'TEXT'}

    if not args.layer:
        print("workspace: {}".format(arcpy.env.workspace))
        for dataset in arcpy.ListDatasets():
            print("  dataset: {}".format(dataset))
            print("    feature classes: {} ".format(arcpy.ListFeatureClasses(feature_dataset=dataset)))
    
        print("  feature classes: {} ".format(arcpy.ListFeatureClasses()))
        print("  tables: {} ".format(arcpy.ListTables()))

    if args.layer in arcpy.ListFeatureClasses() or args.layer in arcpy.ListTables():
        # convert field types as needed
        for field in arcpy.ListFields(args.layer):
            for update_field in field_types.keys():
                if field.name.lower() == update_field:
                    new_type = field_types[update_field]
                    f_name = field.name
                    f_aliasName = field.aliasName
                    # create a new field with the correct data type
                    arcpy.AddField_management(args.layer, "temp", new_type)
                    # calculate the value based on the old field
                    arcpy.CalculateField_management(args.layer, "temp", 'int(round(!field!))', "PYTHON3")
                    # delete the old field
                    arcpy.AlterField_management(args.layer, f_name, f_name+"_old", f_aliasName+"_old")
                    # rename the new field
                    arcpy.AlterField_management(args.layer, "temp", f_name, f_aliasName)

    if args.layer in arcpy.ListFeatureClasses():

        result = arcpy.GetCount_management(os.path.join(args.geodatabase, args.layer))
        print("Feature Class [{}] has {} rows".format(os.path.join(args.geodatabase, args.layer), result[0]))

        if args.format == "geojson":
            outfile = "{}.geojson".format(args.layer)
            arcpy.FeaturesToJSON_conversion(os.path.join(args.geodatabase, args.layer), outfile, geoJSON='GEOJSON')
            print("Wrote {}".format(outfile))

        if args.format == "shp":
            outfile = "{}.shp".format(args.layer)
            arcpy.FeatureClassToShapefile_conversion(os.path.join(args.geodatabase, args.layer), Output_Folder=".")
            print("Wrote {}".format(outfile))

        if args.format == "csv":
            outfile = os.path.join(".","{}.csv".format(args.layer))
            arcpy.CopyRows_management(os.path.join(args.geodatabase, args.layer), outfile)
            print("Wrote {}".format(outfile))

    if args.layer in arcpy.ListTables():

        result = arcpy.GetCount_management(os.path.join(args.geodatabase, args.layer))
        print("Table [{}] has {} rows".format(os.path.join(args.geodatabase, args.layer), result[0]))

        if args.format == "csv":
            outfile = "{}.csv".format(args.layer)
            arcpy.TableToTable_conversion(os.path.join(args.geodatabase, args.layer), out_path=".", out_name=outfile)
            print("Write {}".format(outfile))

        if args.format == "dbf":
            outfile = "{}.dbf".format(args.layer)
            arcpy.TableToTable_conversion(os.path.join(args.geodatabase, args.layer), out_path=".", out_name=outfile)
            print("Wrote {}".format(outfile))

