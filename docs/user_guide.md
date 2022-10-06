# User Guide

This User Guide applies to UrbanSim implementation for the Bay Area. Documentation for the UrbanSim framework is available here.

## Installation

Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended.

1. Install the Anaconda Python distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository
3. Download base data from this Box folder and move the files to `bayarea_urbansim/data/` (ask an MTC contact for access)
4. Clone the MTC urban_data_internal repository to the same location as this repository (ask an MTC contact for access)
5. Create a Python environment with the current dependencies: `conda env create -f baus-env-2020.yml`
6. Activate the environment: `conda activate baus-env-2020`
7. Pre-process the base data: `python baus.py --mode preprocessing` (only needed once)
8. Run the model: `python baus.py` (typical AWS linux run uses `nohup python baus.py -s 25 --disable-slack --random-seed &` which add no hanging up / specifies scenario 25 / disables slack output / turns OFF random seed / puts in background)

More info about the command line arguments: `python baus.py --help`

Optional visualization tool and Slack messenger:

* Configure Amazon Web Services (AWS) to get s3 permission (you will need an appropriately configured AWS credentials file from your MTC contact)
* Install AWS SDK for Python -- boto3 using `pip install boto3`
* Install Slacker to use Slack API using `pip install slacker` (you will need an appropriate slack token to access the slack bot from your MTC contact)
* Set environment variable `URBANSIM_SLACK = TRUE`

## File Structure

TBD
