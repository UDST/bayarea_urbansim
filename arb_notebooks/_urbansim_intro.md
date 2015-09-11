Sam's intro to UrbanSim
-----

#### Official documentation

* http://udst.github.io/urbansim/
* http://urbansim.org

#### What is UrbanSim?

* It's a framework for forecasting patterns of real estate development, residential population, and employment, using statistical models of real estate markets and household/firm/developer decision making

* It operates at the level of individual parcels and agents, but the results are best understood in aggregate

* It's primarily used by Metropolitan Planning Organizations (MPOs) to evaluate alternative scenarios for land use policy and transportation investment

#### Mechanics of a simulation: model steps and data tables

* UrbanSim runs by looping through a sequence of (totally customizable) model steps... here are some typical steps for a residential-only simulation: 

	* **Home Price Hedonic**  
	Predicts a market value for each housing unit based on building and neighborhood characteristics
	
	* **Household Relocation**  
	Predicts which households will choose to move out within the model cycle, using a random draw informed by household characteristics
	
	* **Household Transition**  
	Adds and removes households based on assumptions about in-migration and demographic transition
	
	* **Household Location Choice**  
	Allocates the unplaced households to empty housing units using multinomial logit
	
	* **Development Feasibility**  
	Calculates the profitability of potential development on each empty or underutilized parcel based on zoning and development pro-formas
	
	* **Residential Development**  
	Creates or replaces buildings based on feasibility and probabilistic selection criteria
	
	* Then repeat from the beginning for the next model iteration

* Behind the scenes is a set of data tables which are continually updated to reflect the state of the model

* A huge amount of work goes into setting up the initial data tables: parcels, buildings, zoning, home sales, street networks, a synthetic population of households, and so on

#### Calibration and validation

* Each model step is calibrated from empirical data, generally by estimating an OLS or logit model and using the parameters for the simulation stage

* The simulation as a whole is calibrated in a variety of more ad-hoc ways which I'm not familiar with yet

#### Difficulty of making changes

* Changing regression specifications or adding data attributes is generally easy

* Adding new abstractions (housing tenure, rent control, etc) is more difficult because it requires changes to all the model steps whose input or output is affected

#### Who works on UrbanSim?

* It's an open-source project that's part of the Urban Data Science Toolkit created by Synthicity and now managed by Autodesk

	* Paul's team at Autodesk (software engineering and practical applications)
	* Paul's lab at Berkeley (research extensions)
	* Modeling teams at MTC/ABAG (BayArea_UrbanSim)
	* Other regional planning agencies
	* Other researchers, mostly working on their own forks

#### How do you actually use it?

* UrbanSim is a set of python code libraries, some containing core functionality and others that set up the model steps and data relationships for a particular project

* You run a simulation by executing a small script file that specifies the model steps to invoke and the number of iterations

* Output is saved to text files or can be visualized through a web browser interface

* Code libraries:

	* "[**Orca**](https://github.com/udst/orca/)" - high level wrappers for the model steps and data tables
	* "[**UrbanSim**](https://github.com/udst/urbansim/)" - logic for core components like MNL models, developer pro-formas, etc
	* "[**UrbanSim\_Defaults**](https://github.com/udst/urbansim_defaults/)" - standard definitions for the model steps and data relationships
	* "[**BayArea\_UrbanSim**](https://github.com/udst/bayarea_urbansim/)" - this is where all the project-specific code goes, including the input data and model step logic to augment or override UrbanSim\_Defaults

* MTC manages BayArea\_UrbanSim, so we work from forks and branches of the master code base (e.g. [displacement project](https://github.com/ual/bayarea_urbansim/tree/arb/))

* Various other python libraries are used behind the scenes: pandas for data manipulation, statsmodels for regressions, pandana for network aggregations

* The Orca and UrbanSim codebases are managed as proper software packages with unit tests, API documentation, installation procedures, etc, but UrbanSim\_Defaults and BayArea\_UrbanSim are not

#### Code structure

* The model steps and data relationships are defined in `datasources.py`, `variables.py`, and `models.py` -- definitions in the BayArea\_UrbanSim copies override any identically named definitions in the UrbanSim\_Defaults copies

* Regression specifications and other common settings can be adjusted in the `configs` directory

* Master lists of the estimation steps and simulation steps are set in `Estimation.py` and `Simulation.py`, and running those files sequentially will set up and then execute the model (parameter estimates are stored in the configs for re-use)

* It can be convenient to use IPython notebooks for data exploration and troubleshooting as you're developing a model; see examples in GitHub


