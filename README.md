Bay Area UrbanSim (BAUS) Implementation
=======

This is the UrbanSim implementation for the Bay Area. Documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/). All documentation for Bay Area Urbansim is at: http://bayareametro.github.io/bayarea_urbansim/main/

## Installation
Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended. 

1. Install the Anaconda Python distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository 
3. Create a Python environment with the current dependencies: `conda env create -f baus-env-2020.yml`
4. Activate the environment: `conda activate baus-env-2020`
6. Store `run_setup.yaml` next to the repository (ask an MTC contact for access) and use it to specify the `inputs` and `outputs` folder locations and `run_name`
7. Pull inputs into the model `inputs` folder (ask an MTC contact for access)
8. Establish an outputs folder in the `outputs` folder using the `run_name` as the folder name, and copy the outputs folder structure into it (ask an MTC contact for access)
10. Run `python baus.py` from the main model directory (more info about the command line arguments: `python baus.py --help`)