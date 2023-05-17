DRAFT Bay Area UrbanSim (BAUS) Implementation
=======

[![Build Status](https://travis-ci.org/UDST/bayarea_urbansim.svg?branch=master)](https://travis-ci.org/UDST/bayarea_urbansim)

This is the DRAFT UrbanSim implementation for the Bay Area. Policy documentation for the Bay Area model is available [here](https://bayareametro.github.io/bayarea_urbansim/main/pba50/) and documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/).

### Branches
* `main` contains the most recent release
* `develop` is a staging branch used in high production phases
* feature branches contain descriptive names
* application branches contain descriptive names

### Installation

Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended.

1. Install the [Anaconda Python](https://www.anaconda.com/products/individual#Downloads) distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository
3. Download base data from this [Box folder](under construction) and move the files to `` (ask an MTC contact for access)
4. Create a Python environment with the current dependencies: `conda env create -f baus-env-2020.yml`
5. Activate the environment: `conda activate baus-env-2020`
6. Use `run_setup.yaml` to set `inputs` and `outputs` folder paths and configure a model run
7. Run the model: `python baus.py` 

More info about the command line arguments: `python baus.py --help`

Optional visualization tool and Slack messenger: 
* Configure Amazon Web Services (AWS) to get s3 permission (you will need an appropriately configured AWS credentials file from your MTC contact) 
* Install AWS SDK for Python -- boto3 using `pip install boto3`
* Install Slacker to use Slack API using `pip install slacker` (you will need an appropriate slack token to access the slack bot from your MTC contact)
* Set environment variable `URBANSIM_SLACK = TRUE`

For all other information on the code and model application: http://bayareametro.github.io/bayarea_urbansim/main/