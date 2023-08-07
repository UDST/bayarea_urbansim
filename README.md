Bay Area UrbanSim (BAUS) Implementation
=======

This is the UrbanSim implementation for the Bay Area. Documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/). 

All documentation for Bay Area Urbansim is at: http://bayareametro.github.io/bayarea_urbansim/main/


### Installation
Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended. 
1. Install the Anaconda Python distribution (not strictly required, but makes things easier and more reliable) 
2. Clone this repository 
3. Create a Python environment with the current dependencies: conda env create -f baus-env-2020.yml 
4. Activate the environment: conda activate baus-env-2020


## Model Run
1. Download the model inputs folder, outputs folder structure, and run_setup.yaml file (ask an MTC contact for access)
2. Specify I/O folder locations, model features to enable, and policy configurations in `run_setup.yaml`
4. Run `python baus.py` from the main model directory (more info about the command line arguments: `python baus.py --help`)


### Integration Testing
[![Build Status](https://travis-ci.org/UDST/bayarea_urbansim.svg?branch=master)](https://travis-ci.org/UDST/bayarea_urbansim)