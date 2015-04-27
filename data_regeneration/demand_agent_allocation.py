import pandas as pd, numpy as np
import pandas.io.sql as sql
from spandex import TableLoader
from spandex.io import exec_sql,  df_to_db

#Connect to the database
loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

def unit_choice(chooser_ids, alternative_ids, probabilities):
    """
    Have a set of choosers choose from among alternatives according
    to a probability distribution. Choice is binary: each
    alternative can only be chosen once.

    Parameters
    ----------
    chooser_ids : 1d array_like
        Array of IDs of the agents that are making choices.
    alternative_ids : 1d array_like
        Array of IDs of alternatives among which agents are making choices.
    probabilities : 1d array_like
        The probability that an agent will choose an alternative.
        Must be the same shape as `alternative_ids`. Unavailable
        alternatives should have a probability of 0.

    Returns
    -------
    choices : pandas.Series
        Mapping of chooser ID to alternative ID. Some choosers
        will map to a nan value when there are not enough alternatives
        for all the choosers.

    """
    chooser_ids = np.asanyarray(chooser_ids)
    alternative_ids = np.asanyarray(alternative_ids)
    probabilities = np.asanyarray(probabilities)

    choices = pd.Series([np.nan] * len(chooser_ids), index=chooser_ids)

    if probabilities.sum() == 0:
        # return all nan if there are no available units
        return choices

    # probabilities need to sum to 1 for np.random.choice
    probabilities = probabilities / probabilities.sum()

    # need to see if there are as many available alternatives as choosers
    n_available = np.count_nonzero(probabilities)
    n_choosers = len(chooser_ids)
    n_to_choose = n_choosers if n_choosers < n_available else n_available

    chosen = np.random.choice(
        alternative_ids, size=n_to_choose, replace=False, p=probabilities)

    # if there are fewer available units than choosers we need to pick
    # which choosers get a unit
    if n_to_choose == n_available:
        chooser_ids = np.random.choice(
            chooser_ids, size=n_to_choose, replace=False)

    choices[chooser_ids] = chosen

    return choices

# Load TAZ-level synthetic population
hh_path = loader.get_path('hh/synth/hhFile.p2011s3a1.2010.csv')
hh = pd.read_csv(hh_path)
hh = hh.set_index('HHID')
hh.index.name = 'household_id'
hh = hh.rename(columns = {'TAZ':'taz'})
hh['building_id'] = -1

# Get the taz-level dwelling unit controls just for reference.  This file also contains the employment totals by sector/zone.
taz_controls_csv = loader.get_path('hh/taz2010_imputation.csv')
targetunits = pd.read_csv(taz_controls_csv, index_col='taz1454')

targetunits['hh'] = hh.groupby('taz').size()

df = targetunits[['targetunits', 'hh']]

df['occupancy'] = df.hh*1.0/df.targetunits

print 'Number of zones over-occupied with households will be: %s' % (df.occupancy>1).sum()

##Get the buildings, the alternatives we will be allocating to
buildings = db_to_df('select * from buildings;')
buildings = buildings.set_index('building_id')
empty_units = buildings[buildings.residential_units>0].residential_units.order(ascending=False)
alternatives = buildings[['development_type_id','parcel_id','taz']]
alternatives = alternatives.ix[np.repeat(empty_units.index.values,empty_units.values.astype('int'))]

taz_hh_counts = hh.groupby('taz').size()

for taz in np.unique(hh.taz):
    num_hh = taz_hh_counts[taz_hh_counts.index.values==taz].values[0]
    chooser_ids = hh.index[hh.taz==taz].values
    print 'There are %s households in TAZ %s' % (num_hh, taz)
    alts = alternatives[alternatives.taz==taz]
    alternative_ids = alts.index.values
    probabilities = np.ones(len(alternative_ids))
    num_resunits = len(alts)
    print 'There are %s residential units in TAZ %s' % (num_resunits, taz)
    choices = unit_choice(chooser_ids,alternative_ids,probabilities)
    #households_urbansim.building_id[np.in1d(households_urbansim.index.values,chooser_ids)] = choices.values
    hh.loc[chooser_ids,'building_id'] = choices
    if num_hh > num_resunits:
        print 'Warning:  number of households exceeds number of resunits in TAZ %s' % taz
targetunits['hh_allocated'] = pd.merge(hh, buildings, left_on = 'building_id', right_index = True).groupby('taz_x').size()

df = targetunits[['targetunits', 'hh', 'hh_allocated']]
df['occupancy'] = df.hh*1.0/df.targetunits

print df.head()
summary_output_path = loader.get_path('out/regeneration/summaries/hh_summary.csv')
df.to_csv(summary_output_path)



################
#####JOBS#######
################

sector_columns = []
for col in targetunits.columns:
    if col.startswith('e'):
        if col.endswith('_10'):
            sector_columns.append(col)

emp_targets = targetunits[sector_columns]


total_jobs = int(emp_targets.etot_10.sum())

job_id = np.int32(np.arange(total_jobs) + 1)

taz_id = np.int64(np.zeros(total_jobs))

sector_id = np.int32(np.zeros(total_jobs))

## Prepare jobs table
i = 0
#regional_data = regional_data[regional_data.block11.notnull()].fillna(0)
if 'etot_10' in sector_columns: sector_columns.remove('etot_10')
for taz in emp_targets.index.values:
    for sector in sector_columns:
        num_jobs = int(emp_targets.loc[taz, sector])
        if num_jobs > 0:
            j = i + num_jobs
            taz_id[i:j]=taz
            sector_num = int(sector.split('_')[0].split('e')[1])        
            sector_id[i:j]=sector_num
            i = j
            
jobs_table = {'job_id':job_id,'taz':taz_id,'sector_id':sector_id}

jobs_table = pd.DataFrame(jobs_table)
jobs_table = jobs_table.set_index('job_id')
jobs_table['building_id'] = -1

taz_job_counts = jobs_table.groupby('taz').size()

#buildiing_sqft_per_job assumptions for the initial allocation
building_sqft_per_job = {'BR':355,
 'GV':355,
 'HO':1161,
 'HP':355,
 'IH':661,
 'IL':661,
 'IW':661,
 'MF':400,
 'MR':383,
 'OF':355,
 'RT':445,
 'SC':470,
 'SF':400,
 'LD':1000,
 'VP':1000,
 'other':1000}

##Calculate job spaces per building
buildings['sqft_per_job'] = buildings.development_type_id.map(building_sqft_per_job)
buildings['job_spaces'] = (buildings.non_residential_sqft / buildings.sqft_per_job).fillna(0).astype('int')

##Universe of job space alternatives
empty_units = buildings[buildings.job_spaces > 0].job_spaces.order(ascending=False)
alternatives = buildings[['development_type_id','parcel_id','taz']]
alternatives = alternatives.ix[np.repeat(empty_units.index.values,empty_units.values.astype('int'))]

jobs = jobs_table

##Allocate jobs from TAZ to building
for taz in np.unique(jobs.taz):
    num_jobs = taz_job_counts[taz_job_counts.index.values==taz].values[0]
    chooser_ids = jobs.index[jobs.taz==taz].values
    print 'There are %s jobs in TAZ %s' % (num_jobs, taz)
    alts = alternatives[alternatives.taz==taz]
    alternative_ids = alts.index.values
    probabilities = np.ones(len(alternative_ids))
    num_jobspaces = len(alts)
    print 'There are %s job spaces in TAZ %s' % (num_jobspaces, taz)
    choices = unit_choice(chooser_ids,alternative_ids,probabilities)
    jobs.loc[chooser_ids,'building_id'] = choices
    if num_jobs > num_jobspaces:
        print 'Warning:  number of jobs exceeds number of job spaces in TAZ %s' % taz

targetunits['jobs_allocated'] = pd.merge(jobs, buildings, left_on = 'building_id', right_index = True).groupby('taz_x').size()
targetunits['jobs'] = jobs.groupby('taz').size()
targetunits['job_spaces'] = buildings.groupby('taz').job_spaces.sum()

df = targetunits[['job_spaces', 'jobs', 'jobs_allocated']]
df['occupancy'] = df.jobs_allocated*1.0/df.job_spaces
df['diff'] = df.jobs - df.job_spaces
summary_output_path = loader.get_path('out/regeneration/summaries/emp_summary.csv')
df.to_csv(summary_output_path)

print df.head(50)

print jobs.building_id.isnull().sum()

print hh.building_id.isnull().sum()

targetunits['sqft_per_job'] = targetunits.targetnonressqft/targetunits.etot_10

print targetunits['sqft_per_job'].describe()

jobs.building_id[jobs.building_id.isnull()] = -1
hh.building_id[hh.building_id.isnull()] = -1

#EXPORT DEMAND AGENTS TO DB
df_to_db(jobs, 'jobs', schema=loader.tables.public)
df_to_db(hh, 'households', schema=loader.tables.public)

#EXPORT BUILDING TABLE BACK TO DB
buildings['residential_sqft'] = buildings.residential_units * buildings.sqft_per_unit
buildings2 = buildings[['parcel_id', 'development_type_id', 'improvement_value', 'residential_units', 
                        'residential_sqft', 'sqft_per_unit', 'non_residential_sqft', 'building_sqft', 'nonres_rent_per_sqft', 'res_price_per_sqft', 'stories', 'year_built',
                        'redfin_sale_price', 'redfin_sale_year', 'redfin_home_type', 'costar_property_type', 'costar_rent']]
devtype_devid_xref = {'SF':1, 'MF':2, 'MFS':3, 'MH':4, 'MR':5, 'GQ':6, 'RT':7, 'BR':8, 'HO':9, 'OF':10, 'OR':11, 'HP':12, 'IW':13, 
                      'IL':14, 'IH':15, 'VY':16, 'SC':17, 'SH':18, 'GV':19, 'VP':20, 'PG':21, 'PL':22, 'AP':23, 'LD':24, 'other':-1}
for dev in devtype_devid_xref.keys():
    buildings2.development_type_id[buildings2.development_type_id == dev] = devtype_devid_xref[dev]
buildings2.development_type_id = buildings2.development_type_id.astype('int')
buildings2.residential_units = buildings2.residential_units.astype('int')
buildings2.residential_sqft = buildings2.residential_sqft.astype('int')
buildings2.non_residential_sqft = np.round(buildings2.non_residential_sqft).astype('int')
buildings2.stories = np.ceil(buildings2.stories).astype('int')
buildings2.year_built = np.round(buildings2.year_built).astype('int')
df_to_db(buildings2, 'building', schema=loader.tables.public)