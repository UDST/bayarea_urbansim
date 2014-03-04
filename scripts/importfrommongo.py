from monary import Monary
import numpy as np, pandas as pd
import time
mon = Monary()
columns = ['x','y','shape_area','zone_id','county_id','parcel_id']
t1 = time.time()
numpy_arrays = mon.query('bayarea','parcels',{},columns,['float32']*len(columns))
df = np.matrix(numpy_arrays).transpose() 
df = pd.DataFrame(df, columns=columns)
print time.time()-t1
print df.columns
print df.head()
print df.county_id.value_counts()
#print df.describe()
