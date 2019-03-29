import pandas as pd

py2_store = pd.HDFStore('../output/model_data_output.h5')

years = ['2010', '2025']

for table in py2_store.keys():
    for year in years:
        print(year)
        if year in table:
            table_name = table.split('/')[2]
            print(table_name)
            df = py2_store[table]
            df.to_csv(
                '/home/data/spring_2019/{0}/{1}.csv'.format(year, table_name))
