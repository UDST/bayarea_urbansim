# Bay Area UrbanSim (BAUS)

## Model Overview
Bay Area UrbanSim (BAUS) is a microsimulation land use model used to forecast intraregional growth, study urban policies, and evaluate transportation projects at the Metropolitan Transportation Commission (MTC). UrbanSim simulates the movement of households and firms within the region and the construction of new buildings to hold those households and firms. In this manner, it is used to incrementally forecast potential future urban growth trajectories.

BAUS is the middle model in an interactive suite of three model systems maintained by MTC: 
* Regional economic and demographic information is supplied to BAUS from the REMI CGE model and related demographic processing scripts. BAUS outputs on housing production are used to adjust regional housing prices (and thus other variables) in REMI. 
* Accessibillity information is supplied to BAUS from the Travel Model and influences real estate prices and household and employee location choices. BAUS outputs on the location of various types of households and employees are used to establish origins and destinations in the Travel Model.

### BAUS Sub-Model Flow
A series of sub-models generate the final model forecast. Sub-models run for 5-year periods, starting in the model's base year and running until the model's horizon year. The model's run file (baus.py) specifies these sub-models and the order in which they run. BAUS adds features on top of the core UrbanSim models, simulating real estate decisions such as subsidized housing and 

* Earthquake: Simulates the impact of an earthquake by destroying buildings based on their characteristics, displacing their inhabitants, and leaving them open to redevelopment.

* Sea level rise: Simulates the impact of earthquakes and sea level rise by destroying buildings, displacing their inhabitants, and marking these parcels off-limits.

* Accessibility: In addition to travel model accessibility influence, Pandana is used to calculate "distance to location" variables. Both local (local street network from OSM) and regional (travel model network) networks are used to compute accessibility variables that influence price and location sub-models.

* Housing Prices & Rents: Hedonic regression models are applied to current model year conditions to estimate prices and rents in that year.

* Household & Firm Relocation: Households and employeees are selected to move based on relocatin probabilities. Household move-out choice is conditional on tenure status.

* Household & Firm Transition: Additiional households and employees are added or subtracted from the region to reflect the exogenous control totals. New households and firms are added to the set of relocation households looking for homes and job space.

* Development Pipeline Projects: Buildings in the region's development pipeline are constructed. These are often large approved development projects and development that has occurred after the model's base year.

* Market-rate Developer: The for-profit real estate development process is simulated and new buildings are built where they are most feasible; this also build deed-restricted unit that are produced through the market-rate process (e.g., inclusionary zoning). This model also adds ground floor retail to tall buildings to simulate standard policy.

* Affordable Housing Developer: In many simulations, a similar not-for-profit real estate development process produces affordable housing units based on money available within BAUS Accounts (e.g., bond measures).

* Household & Firm Location Choice: Households and firms are assigned to new locations based on logistic regresssion models that capture the preferences of particular segments (e.g., lower income households, retail jobs). 

* Proportional Jobs Model: Particulary industry sectors which don't follow traditional market economics are forecast separately. For government and education sites, the number of jobs in these locations can grow over the simulation period, but the locations off-limits from redevlopment.

* Retail Model: Takes into account where demand for retail is high (income x households) but supply is low.


###  BAUS Scenarios
BAUS model runs modify policies and other levers to observe potential outcomes. These policies including upzoning, affordable housing subsidization, inclusionary housing, and more. Packages of changes are simulated to forecast impact on the future urban landscape and enterered into the travel model to predict future year travel patterns and greenhouse gas emmissions. For details on how each policy is implemented in the BAUS code, see [Policy Implementation]. 