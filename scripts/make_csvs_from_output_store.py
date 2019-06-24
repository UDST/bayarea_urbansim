import pandas as pd
import argparse
import os

year = 2010

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Make csvs from H5 store.')

    parser.add_argument('--year', '-y', action='store',
                        help='specify the simulation year')
    parser.add_argument(
        '--datastore-filepath', '-d', action='store',
        dest='datastore_filepath',
        help='full pandas-compatible path to the input data file',
        required=True)
    parser.add_argument(
        '--output-data-dir', '-o', action='store', dest='output_data_dir',
        help='full path to the LOCAL output data directory',
        required=True)

    options = parser.parse_args()

    if options.year:
        year = options.year
    datastore_filepath = options.datastore_filepath
    output_data_dir = options.output_data_dir

    py2_store = pd.HDFStore(datastore_filepath)

    for table in py2_store.keys():
        if year in table:
            print('year: {0}'.format(year))

            table_name = table.split('/')[2]
            print('table: {0}'.format(table_name))
            df = py2_store[table]

            if not os.path.exists(output_data_dir):
                os.makedirs(output_data_dir)

            fname = '{0}.csv'.format(table_name)
            output_filepath = os.path.join(output_data_dir, fname)

            if os.path.exists(output_filepath):
                os.remove(output_filepath)

            df.to_csv(output_filepath)
