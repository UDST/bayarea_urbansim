# Outputs


## Core Outputs

TBD

## Policy Outputs

TBD

## For Travel Model
TBD

## Diagnostic Outputs
The table below contains brief descriptions of the disgnostic output tables of BAUS.

| **File name** | **Purpose** | **File type** |
|---------------|-------------|---------------|
| [`simulation_output.json`](#simulation_outputjson-zone-level-diagnostic-attributes) | Two sets of zone-level attributes: one set is the same as the standard Travel Model output, the other set is diagnostic attributes representing development capacity, pricing, logsum, vacancy, development type and quantity, etc., used to help analyze/debug submodels. |JSON |
| `parcel_output.csv` | Parcel-level attributes of parcels with development activities during the simulation time period. |CSV |
| `dropping_buildings.csv` | XXX |CSV |

### `simulation_output.json` zone-level diagnostic attributes:

| **Attribute** | **Description** | **Data Type** | **Source** | **Sub-model/step** |
|-----------------|-----------|--------------|--------------|--------------|
| gid | XXX | XXX | XXX | XXX |
| tract | XXX | XXX | XXX | XXX |
| area | XXX | XXX | XXX | XXX |
| acres | XXX | XXX | XXX | XXX |
| price_shifters | XXX | XXX | XXX | XXX |
| unit_residential_price | median residential price of all residential units in a TAZ | float | BAUS "residential_units" table | [rsh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L745) |
| unit_residential_rent | median residential monthly rent of all residential units in a TAZ | float | BAUS "residential_units" table | [rrh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L764) |
| unit_residential_price_>_rent | 1 or 0 representing if the TAZ-level per-unit residential price is higher than annualized rent divided by [cap_rate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/configs/developer/developer_settings.yaml#L2). | int | BAUS "residential_units" table and "developer_settings" | [summary.py](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/summaries.py#L265) |
| residential_price | median per sq.ft. residential price of all residential buildings in a TAZ (based on general building type); currently used only for estimation, not for simulation | float | "buildings" table  | [residential_price()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L248) |
| retail_rent | median non_residential_rent of all retail buildings in a TAZ | float | BAUS "buildings" table  | [nrh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L708) |
| office_rent | median non_residential_rent of all office buildings in a TAZ | float | BAUS "buildings" table  | [nrh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L708) |
| industrial_rent | median non_residential_rent of all industrial buildings in a TAZ | float | BAUS "buildings" table  | [nrh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L708) |
| zone_cml | zone-level mandatory accessibilities | float | Travel Model; BAUS "zones" table | [zone_cml()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/variables.py#L930) |
| zone_cnml | zone-level non-mandatory accessibilities | float | Travel Model; BAUS "zones" table | [zone_cnml()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/variables.py#L948) |
| zone_combo_logsum | zone-level total accessibilities | float | Travel Model; BAUS "zones" table | [zone_combo_logsum()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/variables.py#L966) |
| zoned_du | maximum number of dwelling units allowed on all parcels in a TAZ | int | BAUS "parcels" table | [zoned_du()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L957) |
| zoned_du_underbuild | additional dwelling units allowed on all parcels in a TAZ given existing development conditions, 0 if the additional units are not at least half of existing development | int | BAUS "parcels" table | [zoned_du_underbuild()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L1037) 
| zoned_du_underbuild_ratio | ratio of additional allowable residential units to maximum allowable residential units | int | BAUS "zones" table | [zoned_du_build_ratio()]() |
| building_count | total number of residential buildings in a TAZ, based on general building type | int | BAUS "buildings" table | XXX |
| residential_units | total number of residential units in a TAZ | int | BAUS "buildings" table | XXX |
| ave_unit_sqft | 0.6 quantile of building-level average residential unit size of all builidings in a TAZ | int | BAUS "building" table | [ave_unit_sqft()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L943) |
| residential_vacancy | percentage of residential units in a TAZ that are not occupied by a household | float | BAUS "households" table, BAUS "buildings" table | BAUS [summary.py]() |
| non_residential_sqft | total sq.ft. of all non-residential buildings in a TAZ | int | BAUS "buildings" table | XXX |
| job_spaces | total number of jobs that can be accommodated by non-residential space in a TAZ | int | BAUS "building" table; sq.ft. per job setting | XXX |
| non_residential_vacancy | percentage of job_spaces in a TAZ that are not occupied by a job | float | BAUS "jobs" table, BAUS "buildings" table | XXX |
| retail_sqft | total sq.ft. of all retail buildings in a TAZ | int | BAUS "buildings" table | XXX |
| retail_to_res_units_ratio | ratio of total retail sq.ft. to total residential units in a TAZ | int | BAUS "parcels" table | XXX |
| office_sqft | total sq.ft. of all office buildings in a TAZ | int | BAUS "buildings" table | XXX |
| industrial_sqft | total sq.ft. of all industrial buildings in a TAZ | int | BAUS "buildings" table | XXX |
| household_size | median household size of all households in a TAZ | float | BAUS "households" table; BAUS "households_preproc" table | XXX |
| average_income | median income of all households in a TAZ | int | BAUS "households" table; BAUS "households_preproc" table | [summary.py](); MTC/ABAG household models? |

### `parcel_output.csv` parcel-level attributes:

| **Attribute** | **Description** | **Data Type** | **Source** | **Sub-model/step** |
|-----------------|-----------|--------------|--------------|--------------|
| XXX | XXX | XXX | XXX | XXX |

### `dropping_buildings.csv` building-level attributes:

| **Attribute** | **Description** | **Data Type** | **Source** | **Sub-model/step** |
|-----------------|-----------|--------------|--------------|--------------|
| XXX | XXX | XXX | XXX | XXX |