from monary import Monary
import numpy as np
import pandas as pd
import time

mon = Monary()

columns = [
    'properties.total_residential_units',
    'properties.total_job_spaces',
    'properties.parcel_id',
    'properties.max_dua',
    'properties.max_far'
]

t1 = time.time()

numpy_arrays = mon.query(
    'togethermap',
    'places',
    {'collectionId': 'ZC7yyAyA8jkDFnRtf'},
    columns,
    ['float32']*len(columns)
)

df = np.matrix(numpy_arrays).transpose()
df = pd.DataFrame(df, columns=columns)

print(time.time()-t1)
print(df.describe())
