import pandas as pd

py2_store = pd.HDFStore('./output/model_data_output_2040.h5')

years = ['2040']
scenario = 'b-ht'

print('scenario: {0}'.format(scenario))
for table in py2_store.keys():
    for year in years:
        if year in table:
            print(year)
            table_name = table.split('/')[2]
            print(table_name)
            df = py2_store[table]
            df.to_csv(
                '/home/data/spring_2019/{0}/{1}/{2}.csv'.format(
                    year, scenario, table_name))
