import urbansim.sim.simulation as sim
import os
import pandas as pd
from urbansim.utils import misc


sim.add_injectable("building_type_map", {
    1: "Residential",
    2: "Residential",
    3: "Residential",
    4: "Office",
    5: "Hotel",
    6: "School",
    7: "Industrial",
    8: "Industrial",
    9: "Industrial",
    10: "Retail",
    11: "Retail",
    12: "Residential",
    13: "Retail",
    14: "Office"
})


sim.add_injectable("form_to_btype", {
    'residential': [1, 2, 3],
    'industrial': [7, 8, 9],
    'retail': [10, 11],
    'office': [4],
    'mixedresidential': [12],
    'mixedoffice': [14],
})


sim.add_injectable("store", pd.HDFStore(os.path.join(misc.data_dir(), "sanfran.h5"), mode="r"))


sim.add_injectable("scenario_inputs", {
    "baseline": {
        "zoning_table_name": "zoning_baseline"
    },
    "test": {
        "zoning_table_name": "zoning_test"
    }
})


sim.add_injectable("scenario", "baseline")