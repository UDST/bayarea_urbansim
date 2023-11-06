USAGE = """

Reads a feature class from an input geodatabase, joins with csv, and exports to new feature class in output geodatabase.

Joins via https://pro.arcgis.com/en/pro-app/tool-reference/data-management/add-join.htm

"""

import argparse, os, sys, time
import arcpy, numpy, pandas

if __name__ == '__main__':

    start = time.time()

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("input_gdb",    metavar="input.gdb",   help="Input geodatabase")
    parser.add_argument("input_layer",  metavar="input_layer", help="Geometry layer in input geodatabase")
    parser.add_argument("input_field",  metavar="input_layer", help="Join field in input_layer")
    parser.add_argument("join_csv",     metavar="join.csv",    help="CSV layer for joining")
    parser.add_argument("join_field",   metavar="join_field",  help="Join field in join_csv")
    parser.add_argument("join_type",    choices=["KEEP_ALL","KEEP_COMMON"], default="KEEP_ALL", help="Outer join vs inner join.  Default is KEEP_ALL, or outer")
    parser.add_argument("output_gdb",   metavar="output.gdb",  help="Output geodatabase ")

    args = parser.parse_args()
    print(" {:15}: {}".format("input_gdb",   args.input_gdb))
    print(" {:15}: {}".format("input_layer", args.input_layer))
    print(" {:15}: {}".format("input_field", args.input_field))
    print(" {:15}: {}".format("join_csv",    args.join_csv))
    print(" {:15}: {}".format("join_field",  args.join_field))
    print(" {:15}: {}".format("join_type",   args.join_type))
    print(" {:15}: {}".format("output_gdb",  args.output_gdb))

    # our workspace will be the output_gdb
    if not os.path.exists(args.output_gdb):
        (head,tail) = os.path.split(args.output_gdb)
        print("head: {} tail: {}".format(head, tail))
        if head=="": head="."
        arcpy.CreateFileGDB_management(head, tail)
        print("Created {}".format(args.output_gdb))

    arcpy.env.workspace = args.output_gdb

    # read the csv
    df = pandas.read_csv(args.join_csv)
    print("Read {} lines from {}. Head:\n{}Dtypes:\n{}".format(len(df), args.join_csv, df.head(), df.dtypes))

    # copy to the output_gdb as a table
    table_name = os.path.split(args.join_csv)[1]
    table_name = os.path.splitext(table_name)[0]
    # remove leading numbers and _
    table_name = table_name.lstrip("0123456789_-")
    print("Adding to {} as table named {}".format(args.output_gdb, table_name))

    # delete table if there's already one there by that name
    if arcpy.Exists(table_name):
        arcpy.Delete_management(table_name)
        print("Found {} -- deleting".format(table_name))

    df_arr = numpy.array(numpy.rec.fromrecords(df.values))
    df_arr.dtype.names = tuple(df.dtypes.index.tolist())
    arcpy.da.NumPyArrayToTable(df_arr, os.path.join(args.output_gdb, table_name))
    print("Created {}".format(os.path.join(args.output_gdb, table_name)))

    # we shall pare it down to just the Geometry and join_field
    fields = arcpy.ListFields(os.path.join(args.input_gdb, args.input_layer))
    delete_fields = []
    keep_fields   = []
    for field in fields:
        # keep Geometry and join_field
        if field.type == "Geometry" or field.name == args.input_field or field.required:
            keep_fields.append(field.name)
        else:
            delete_fields.append(field.name)
        # print("{0} is a type of {1} with a length of {2}".format(field.name, field.type, field.length))

    print("Keeping fields {}".format(keep_fields))
    print("Deleting fields {}".format(delete_fields))
    # make sure we found both a geometry field and the join_field
    assert(len(keep_fields)>=2)
    assert(args.input_field in keep_fields)
    # delete the fields post join since the join might reduce the size substantially if it's an inner join

    # delete the layer if it already exists in the output gdb
    if arcpy.Exists(args.input_layer):
        arcpy.Delete_management(args.input_layer)
        print("Found {} -- deleting".format(args.input_layer))

    # copy the input to output_gdb with the same name
    arcpy.CopyFeatures_management(os.path.join(args.input_gdb, args.input_layer),
                                  os.path.join(args.output_gdb, args.input_layer))
    # create join layer with input_layer and join_table
    print("Joining {} with {}".format(os.path.join(args.output_gdb, args.input_layer), table_name))
    joined_table = arcpy.AddJoin_management(os.path.join(args.output_gdb, args.input_layer), args.input_field, 
                                            os.path.join(args.output_gdb, table_name), args.join_field,
                                            join_type=args.join_type)

    new_table_name = "{}_joined".format(table_name)

    # delete the layer if it already exists in the output gdb
    if arcpy.Exists(new_table_name):
        arcpy.Delete_management(new_table_name)
        print("Found {} -- deleting".format(new_table_name))

    # save it
    arcpy.CopyFeatures_management(joined_table, os.path.join(args.output_gdb, new_table_name))
    print("Completed creation of {}".format(os.path.join(args.output_gdb, new_table_name)))

    # NOW delete fields
    # do these one at a time since sometimes they fail
    for field in delete_fields:
        try:
            arcpy.DeleteField_management(os.path.join(args.output_gdb, new_table_name), [field])
            print("Deleted field {}".format(field))
        except:
            print("Error deleting field {}: {}".format(field, sys.exc_info()))

    num_rows = arcpy.GetCount_management(os.path.join(args.output_gdb, new_table_name))
    print("{} has {} records".format(new_table_name, num_rows[0]))

    print("Script took {0:0.1f} minutes".format((time.time()-start)/60.0))