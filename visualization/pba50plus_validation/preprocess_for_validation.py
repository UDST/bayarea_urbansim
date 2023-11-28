# # BAUS Contingency Plan validation Tableau preprocessing
# 
# This script produces two CSV tables for subsequent use with the BAUS Contingency Plan validation Tableau workbook:
# 1. taz_data_long.csv - A pure long-format table for superdistrict/county-level comparisons of simple headcounts 
#    (e.g. `TOTHH` or `RETEMPN`)
# 2. taz_data_wide_vacancy.csv - A mixed-format table (source/year/TAZ on the rows, certain values on the columns) 
#    for superdistrict/county-level comparisons of residential and non-residential vacancy rates

# %%
import pandas as pd
from pathlib import Path
import getpass

user = getpass.getuser()

# For comparing with Observed (Census, etc)
BASE_YEARS           = ['2010', '2020', '2023']
# For comparing with PBA50 or betwee runs
KEY_YEARS            = ['2010', '2020', '2025', '2035', '2050']

TRAVEL_MODEL_ONE_DIR = Path(r'C:\Users\{user}\Documents\GitHub\travel-model-one')
FMS_BOX_DIR          = Path(r'C:\Users\{user}\Box\Modeling and Surveys')

# My VM's C Drive ran out of space so these are on E:
if user in ['lzorn']:
    TRAVEL_MODEL_ONE_DIR = Path(r'E:\GitHub\travel-model-one')
    FMS_BOX_DIR          = Path(r'E:\Box\Modeling and Surveys')

COSTAR_2020_TAZ      = FMS_BOX_DIR / 'Urban Modeling\Bay Area UrbanSim\PBA50Plus_Development\Contingency\costar_2020_taz1454.csv'

# %%
# First ingest Census data

census_dir = TRAVEL_MODEL_ONE_DIR / 'utilities' / 'taz-data-baseyears'
melt_vars = [
    'TOTHH',
    'TOTEMP',
    'RES_UNITS',
    'HHINCQ1',
    'HHINCQ2',
    'HHINCQ3',
    'HHINCQ4',
    'AGREMPN',
    'FPSEMPN',
    'HEREMPN',
    'MWTEMPN',
    'OTHEMPN',
    'RETEMPN',
]

dfs = []
vacancy_dfs = []
for year in BASE_YEARS:
    in_df = pd.read_csv(census_dir / year / f'TAZ1454 {year} Land Use.csv')
    
    # 2010 has RES_UNITS already but the other years do not
    if 'RES_UNITS' not in in_df.columns:
        in_df['RES_UNITS'] = in_df['SFDU'] + in_df['MFDU']
    
    # Melt only those columns that are present in this year
    long = in_df.melt(
        id_vars='ZONE',
        value_vars=[var for var in melt_vars if var in in_df.columns]
    )

    long['source'] = 'Census'
    long['year'] = year

    dfs.append(long)

    # Store some data differently for vacancy purposes
    wide = in_df[['ZONE', 'TOTHH', 'RES_UNITS']]
    wide.columns = ['TAZ', 'TOTHH', 'residential_units']
    wide['residential_vacancy'] = 1 - wide['TOTHH'] / wide['residential_units']
    del wide['TOTHH']
    wide['source'] = 'Census/CoStar'
    wide['year'] = year
    vacancy_dfs.append(wide)    


# %%
# Next ingest model run summaries from a variety of file locations

# TODO: Move to csv
scenarios = {
    'PBA50': {
        'path': Path(rf"C:\Users\{user}\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\PBA50\Final Blueprint runs\Final Blueprint (s24)\BAUS v2.25 - FINAL VERSION"),
        'pattern': '*_taz_summaries_*'
    },
    'v0: PBA50 equivalent inputs': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v0"),
        'pattern': 'taz1_summary_*'
    },
    'v1: BASIS buildings in dev pipeline': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v1"),
        'pattern': 'taz1_summary_*'
    },
    'v2: BASIS buildings, updated control totals': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v2"),
        'pattern': 'taz1_summary_*'
    },
    'v3: aligned 2020 HHINC control totals': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v3"),
        'pattern': '*_taz1_summary_*'
    },
    'v4: aligned 2020 HHINC+employment control totals': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v4"),
        'pattern': '*_taz1_summary_*'
    },
    'v4: aligned 2020 HHINC+employment control totals': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v4"),
        'pattern': '*_taz1_summary_*'
    },
    'v5: aligned control totals, updated pipeline': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v5"),
        'pattern': '*_taz1_summary_*'
    },
    'v6: additional pipline updates, costar data, fixed vacancy': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v7"),
        'pattern': '*_taz1_summary_*'
    },
    'v7: housing preservation activated in 2025': {
        'path': Path(r"\\lumodel3\LUModel3Share\baus_main_current_PBA50_inputs\outputs\pba50_fbp_pr319_v8"),
        'pattern': '*_taz1_summary_*'
    }
}

for scenario, params in scenarios.items():
    # We need to handle PBA50 differently from the Contingency Plan runs
    if (params['path'] / 'travel_model_summaries').exists():
        long_dir = params['path'] / 'travel_model_summaries'
    else:  # This is the case for PBA50
        long_dir = params['path']
    
    files_read = 0
    for file in long_dir.glob(params['pattern']):
        if file.stem[-4:] in KEY_YEARS:
            wide = pd.read_csv(file)
            print("Read {:,} rows from {}".format(len(wide), file))
            long = wide.melt(
                id_vars='ZONE',
                value_vars=melt_vars
            )

            long['source'] = scenario
            long['year'] = file.stem[-4:]

            dfs.append(long)
            files_read += 1
    if files_read==0:
        print('WARNING: Found 0 files for {}'.format(scenario))

    # Handle vacancy data
    if (params['path'] / 'core_summaries').exists():
        for file in (params['path'] / 'core_summaries').glob('*_interim_zone_output_*'):
            if file.stem[-4:] in KEY_YEARS:
                vac_df = pd.read_csv(file, usecols=[
                    'TAZ',
                    'residential_units',
                    'residential_vacancy',
                    'non_residential_sqft',
                    'non_residential_vacancy',
                ])
                print("Read {:,} rows from {}".format(len(vac_df), file))

                vac_df['source'] = scenario
                vac_df['year'] = file.stem[-4:]

                vacancy_dfs.append(vac_df)


df = pd.concat(dfs)
vacancy_df = pd.concat(vacancy_dfs)

df


# %%
costar = pd.read_csv(COSTAR_2020_TAZ, dtype={'TAZ': int}, index_col='TAZ')
print("Read {:,} rows from {}".format(len(costar), COSTAR_2020_TAZ))

# %%
vacancy_df = vacancy_df.set_index('TAZ')
vacancy_df

# %%
vacancy_df.loc[(vacancy_df['source'] == 'Census/CoStar') & (vacancy_df['year'] == '2020') & vacancy_df.index.isin(costar.index), ['non_residential_sqft', 'non_residential_vacancy']] = costar
vacancy_df[vacancy_df['year'] == '2020']

# %%
vacancy_df.to_csv('taz_data_wide_vacancy.csv')
print("Wrote {:,} lines to taz_data_wide_vacancy.csv".format(len(vacancy_df)))

# %%
# Linearly interpolate to generate 2023 estimates

for scenario in scenarios.keys():
    values_2020 = df.loc[(df['source'] == scenario) & (df['year'] == '2020'), 'value']
    values_2025 = df.loc[(df['source'] == scenario) & (df['year'] == '2025'), 'value']
    assert len(values_2020) == len(values_2025)
    values_2023 = values_2020 + (values_2025 - values_2020) * (3 / 5)

    # Construct 2023 records
    id_cols = df.loc[(df['source'] == scenario) & (df['year'] == '2020'), ['ZONE', 'variable']]
    df_2023 = pd.concat([id_cols, values_2023], axis=1)
    df_2023['source'] = scenario
    df_2023['year'] = '2023'
    
    dfs.append(df_2023)

# %%
out_df = pd.concat(dfs)

out_df.to_csv('taz_data_long.csv', index=False)
print("Wrote {:,} lines to taz_data_long.csv".format(len(out_df)))

# %%
pd.crosstab(out_df['source'], out_df['year'])


