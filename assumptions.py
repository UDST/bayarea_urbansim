import urbansim.sim.simulation as sim
import os
import pandas as pd
from urbansim.utils import misc


sim.add_injectable("building_sqft_per_job", {
    -1: 400,
    4: 355,
    5: 1161,
    6: 470,
    7: 661,
    8: 960,
    9: 825,
    10: 445,
    11: 445,
    12: 383,
    13: 383,
    14: 383,
})


# this maps building type ids to general building types
# basically just reduces dimensionality
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


# this maps building "forms" from the developer model
# to building types so that when the developer builds a
# "form" this can be converted for storing as a type
# in the building table - in the long run, the developer
# forms and the building types should be the same and the
# developer model should account for the differences
sim.add_injectable("form_to_btype", {
    'residential': [1, 2, 3],
    'industrial': [7, 8, 9],
    'retail': [10, 11],
    'office': [4],
    'mixedresidential': [12],
    'mixedoffice': [14],
})


sim.add_injectable("store", pd.HDFStore(os.path.join(misc.data_dir(),
                                                     "sanfran.h5"), mode="r"))


# this keeps track of all of the inputs that get "switched"
# whenever a scenario is changed
sim.add_injectable("scenario_inputs", {
    "baseline": {
        "zoning_table_name": "zoning_baseline"
    },
    "test": {
        "zoning_table_name": "zoning_test"
    }
})


sim.add_injectable("scenario", "baseline")
