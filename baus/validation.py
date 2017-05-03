import sys
import os
import orca
import pandas as pd
from pandas.util import testing as pdt


@orca.step()
def simulation_validation(buildings, residential_units):
    # assert we fanned out the residential units correctly
    assert len(residential_units) == buildings.residential_units.sum()

    # make sure the unit counts per building add up
    pdt.assert_series_equal(
        buildings.residential_units[
            buildings.residential_units > 0].sort_index(),
        residential_units.building_id.value_counts().sort_index(),
        check_names=False,
        check_dtype=False
    )

    # make sure we moved deed restricted units to the res units table correctly
    pdt.assert_series_equal(
        buildings.deed_restricted_units[
            buildings.residential_units > 0].sort_index(),
        residential_units.deed_restricted.groupby(
            residential_units.building_id).sum().sort_index(),
        check_names=False,
        check_dtype=False
    )