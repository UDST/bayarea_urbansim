Bay Area UrbanSim (BAUS) Implementation
=======

This is the UrbanSim implementation for the Bay Area. Documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/). All documentation for Bay Area Urbansim is at: http://bayareametro.github.io/bayarea_urbansim/main/

## Installation
Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended. 

1. Install the Anaconda Python distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository 
3. Create a Python environment with the current dependencies: `conda env create -f baus-env-2023.yml`
4. Activate the environment: `conda activate baus-env-2023`
6. Store `run_setup.yaml` next to the repository (ask an MTC contact for access) and use it to specify the `inputs` and `outputs` folder locations and `run_name`
7. Pull inputs into the model `inputs` folder (ask an MTC contact for access)
8. Establish an outputs folder in the `outputs` folder using the `run_name` as the folder name, and copy the outputs folder structure into it (ask an MTC contact for access)
10. Run `python baus.py` from the main model directory (more info about the command line arguments: `python baus.py --help`)


## Optional Slack Messenger 
* Install the Slack SDK using `pip install slack_sdk`
* Set up environment variable using the bot token: Right click on “This PC” -> Properties -> Advanced system settings -> Environment Variables -> click “New” under “User variables”.
* Set environment variable `SLACK_TOKEN = token` (you will need an appropriate slack token from your MTC contact)
* Set environment variable `URBANSIM_SLACK = TRUE`


## Optional Model Run Visualizer
* Configure the location that BAUS will write the visualizer files to in `run_setup.yaml` (stored on MTC's Box account for internal visualization)
* Open the visualizer from the BAUS repository to explore the model run, and/or
* Open the visualizer from the BAUS repository and publish it to the web (hosted on using MTC's Tableau). At this time runs can be removed from `model_run_inventory.csv` to select the runs to be shown on the web tool