# same as models need to prune

from urbansim.utils import misc
import os
import sys
import orca
import yaml
import datasources
import variables
from utils import parcel_id_to_geom_id, geom_id_to_parcel_id, add_buildings
from utils import round_series_match_target, groupby_random_choice
from urbansim.utils import networks
import pandana.network as pdna
from urbansim_defaults import models
from urbansim_defaults import utils
from urbansim.developer import sqftproforma, developer
from urbansim.developer.developer import Developer as dev
import subsidies
import summaries
import numpy as np
import pandas as pd

# inundation is parcels where building destroyed and become nodev

#@orca.step()
#def slr_inundate(parcels, slr_parcels):
#
#    # select parcels that are indundated in the current year
#
#    true_slrparcels=slr_parcels.query(
#
#    # remove building space from parcels
#
#    
#     
#    # remove households and jobs and put in unplaced
#
#
#    # create summary table of count of impacted parcels, units, sqft, hhs by type, jobs by sector
#
#
#    # mark parcel as nodev





# floodier is parcels where buildings decline in value maybe
