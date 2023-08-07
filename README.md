Bay Area UrbanSim (BAUS) Implementation
=======

This is the UrbanSim implementation for the Bay Area. Documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/). All documentation for Bay Area Urbansim is at: http://bayareametro.github.io/bayarea_urbansim/main/

## Installation
Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended. 

1. Install the Anaconda Python distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository 
3. Create a Python environment with the current dependencies: `conda env create -f baus-env-2020.yml`
4. Activate the environment: `conda activate baus-env-2020`
6. Store `run_setup.yaml` next to the  repository
7. Specify `inputs` and `outputs` folder locations and `run_name` in `run_setup.yaml`
8. Pull inputs into the model `inputs` folder (ask an MTC contact for access)
9. In the `outputs` folder, use the `run_name` to establish an outputs folder for the run and copy the outputs folder structure into it
10. Run `python baus.py` from the main model directory (more info about the command line arguments: `python baus.py --help`)