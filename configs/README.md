# Configuration and Settings Files


### settings.yaml

Contains general settings that get passed to the model steps. This includes settings for things like rates (e.g. relocation rates, cap rates), kwargs (e.g. residential developer specifications), and value settings (e.g. building sqft per job, ave sqft per unit clip). These settings are removed from the model logic for clarity and ease in modifying.


### policy.yaml

This contains the settings for the various policy features in Bay Area UrbanSim. More on these policies is found on the [Github Pages for this repository](http://bayareametro.github.io/bayarea_urbansim/pba2040/).


### inputs.yaml

For several data inputs the model uses a flexible file selection. These allow the user to specify how input files enter the model.


### mapping.yaml

This is the location to find all variable mapping and lookups.


### hazards.yaml

These are settings that pertain to the Sea Level Rise and Earthquake model. They enable these features as well as control their specfications and policies.






