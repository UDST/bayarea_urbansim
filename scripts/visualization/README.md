This folder contains script to process UrbanSim model run data (inputs, interim data, outputs) for visualization dashboard.

#### [BAUS_vis_onetime_data_prep.ipynb](BAUS_vis_onetime_data_prep.ipynb)
Creates a number of utility files used in the visualization, including, but not limited to, various crosswalks and spatial datasets. This is a temporary script that will likely be integrated into the BASIS process.


#### [prepare_data_for_viz.py](prepare_data_for_viz.py)
Prepares UrbanSim model run data (output, key input, interim) for (Tableau) dashboard visualization. Process one run at a time.
* Example call: `python prepare_data_for_viz.py "C:\Users\ywang\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\PBA50Plus_Development\Clean Code PR #3" "C:\Users\ywang\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\PBA50Plus_Development\Clean Code PR #3\bayarea_urbansim" "s24" "Model Dev PR3" "model development test run for PR #3" "run143" --upload_to_redshift`
