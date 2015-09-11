Sam's intro to UrbanSim
-----

#### Official documentation

* tk

#### What is UrbanSim?

* It's a framework for forecasting spatial patterns of real estate development, residential population, and employment, using statistical models of real estate markets and household/firm/developer decision making
* It operates at the level of individual parcels and agents, but the results are best understood in aggregate
* It's primarily used by Metropolitan Planning Organizations (MPOs) to evaluate alternative scenarios for land use regulation and transportation investment

#### Mechanics of a simulation - model steps and data tables

* UrbanSim loops through a sequence of model steps, which can be entirely customized. Here are some typical steps for residential modeling: 

	* Home Price Hedonic - predict a market value for each housing unit based on building and neighborhood characteristics
	* Household Relocation - predict which households will choose to move out within the model cycle, as a random draw informed by household characteristics
	* Household Transition - add households (people) to the model based on demographic assumptions
	* Household Location Choice - allocate the unplaced households to empty housing units using multinomial logit
	* Development Feasibility - calculate the profitability of potential development on each empty or underutilized parcel based on zoning and development pro-formas
	* Residential Development - create or replace buildings based on feasibility and probabilistic selection criteria
	* Then repeat from the beginning for the next model iteration

* Behind the scenes is a set of data tables which are continually updated to reflect the state of the model -- parcels, buildings, households, street nodes, etc.
* A huge amount of work goes into setting up the initial data tables: parcels, zoning, home sales, street networks, a synthetic population of households, and so on

#### Calibration and validation

* Each model step is calibrated from empirical data, generally by estimating an OLS or logit model and using the parameters for the simulation stage
* The simulation as a whole is calibrated in a variety of more ad-hoc ways which I'm not familiar with yet

#### Difficulty of making changes

* Changing model specifications or adding data attributes is generally easy
* Adding new abstractions (housing tenure, rent control, etc) is more difficult because it requires changes to all the model steps whose input or output is affected

#### How do you actually use it?

* UrbanSim is a set of python code libraries, some containing core functionality and others which are edited to set up the model steps and data relationships for a particular project
* You run a simulation by executing a small script file that specifies which model steps to invoke over how many iterations
* Output is saved to text files or can be visualized through a web browser interface

Code libraries:

* "Orca" - high level wrappers for the model steps and data tables
* "UrbanSim" - logic for core components like MNL models, developer pro-formas, etc
* "UrbanSim_Defaults" - standard definitions for the model steps and data relationships
* "BayArea_UrbanSim" - this is where all the project-specific code goes, including the input data and model step logic to augment or override UrbanSim_Defaults

* MTC manages BayArea_UrbanSim, so we work from fork/branches of the master code base
* Various third-party python libraries are also integral, e.g. pandas, statsmodels, etc

* The Orca and UrbanSim codebases are managed as proper software packages with unit tests, API documentation, installation procedures, etc, but UrbanSim_Defaults and BayArea_UrbanSim are not

#### Who works on UrbanSim?

* It's an open-source project that's part of the Urban Data Science Toolkit created by Synthicity and now managed by Autodesk

* Paul's team at Autodesk (software engineering and practical applications)
* Paul's lab at Berkeley (research extensions)
* Modeling teams at MTC/ABAG (BayArea_UrbanSim)
* Other regional planning agencies
* Other researchers, mostly working on their own forks


