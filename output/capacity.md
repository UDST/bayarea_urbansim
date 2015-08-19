This document describes the zoning capacity calculations.  The purpose of the zoning calculations is to calculate the capacity for development of some building type within jurisdictional zoning regulations.  Although conceptually simple, there is some difficulty in specifying and interpreting this calculation, so over time this document will likely describe several calculations, ranging from simple to complex, and describing them in some detail.  For now, the purpose of this calculation is to simple sum the capacity for residential units within zoning for each jurisdiction of the 9 county Bay Area.

The script to compute capacity is [here](https://github.com/MetropolitanTransportationCommission/bayarea_urbansim/blob/capacity-calculator/scripts/capacity_calculator.py) and runs in about 15 seconds.

The current results for this calculation are available [here])(https://github.com/MetropolitanTransportationCommission/bayarea_urbansim/blob/capacity-calculator/output/city_capacity.csv).

The simplest calculation is computed on each parcel and is defined [here](https://github.com/MetropolitanTransportationCommission/bayarea_urbansim/blob/11041451fdcf1ba8b473428b10e8a8a59c7aba89/variables.py#L146).

In words, this calculation first takes the `max_dua` (max dwelling units per acre) of each parcel where avialable times the number of acres.  When `max_dua` on a parcel is not available, `max_far` (max floor area ratio) is multiplied by the parcel size (in square feet) to get maxiumum building space, which is divided by an assumed unit size (at the time of this writing, the assumption is 1000 sqft).  This is only used when `max_dua` is not available.  Then the combined number for each parcel is set to zero if residential building types are not allowed.  

The result is then grouped by jurisdiction, summed and writted to a csv.  We call this calculation `zoned_du`.  `zoned_du` in concept is the amount of capacity that could be built if there were no buildings currently in existance.  It is thus a metric of *total* capacity and not change in capacity.

The second calculation is `zoned_du_underbuild`, which takes into account the most basic concept of real estate feasibility, often called "soft site analysis" in planning practice.  This calculation removes parcels which do not have enough capacity to build at least 50% more units that are currently on the parcel, assuming that the cost of acquistion of the building and land, demolition, and construction, will not be financial feasible unless selling at the end roughly 150% as many units as are in existance now.  This is probably conservative (i.e. you would need more than 50% more capacity to justify teardown).  The result is thus a metric of *change* in capacity or the number of units that could be built in each jurisdiction in addition to the units that are built now.



