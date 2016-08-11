#!/bin/bash

# This script sets up bayarea_urbansim on a clean instance of OS X or Linux.
# Developed by Sam Maurer, June 2016, tested on OS X v10.11 and Ubuntu v16.
# Updated August 2016

cd ~/Documents  # or whatever directory you want to use

# The first step is to download and install Anaconda. You can replace "MaxOSX" with 
# "Linux" in the filename string as needed. The "-b -p" flags bypass the interactive 
# installation prompts, for automated builds. But on a development machine, you should 
# use the interactive version of the installer, which will set your environment variables 
# to permanently include the Anaconda copy of python.

curl -O http://repo.continuum.io/archive/Anaconda2-4.0.0-MacOSX-x86_64.sh
bash Anaconda2-4.0.0-MacOSX-x86_64.sh -b -p $HOME/anaconda
export PATH="$HOME/anaconda/bin:$PATH"

# Download bayarea_urbansim. (The "git://" prefix downloads pull-only copies of the 
# repositories. On a development machine you should use "https://", but this will
# require you to log into your Github account.)

git clone git://github.com/ual/bayarea_urbansim.git

# Download the other urbansim libraries
git clone git://github.com/udst/orca.git
git clone git://github.com/udst/orca_test.git
git clone git://github.com/udst/pandana.git
git clone git://github.com/udst/urbansim.git
git clone git://github.com/udst/urbansim_defaults.git

# Set up the libraries
cd orca; python setup.py develop; cd ..
cd orca_test; python setup.py develop; cd ..
cd pandana; python setup.py develop; cd ..
cd urbansim; python setup.py develop; cd ..
cd urbansim_defaults; python setup.py develop; cd ..

# Switch to development branches as needed
cd bayarea_urbansim; git checkout ual-development; cd ..

# Download the bayarea_urbansim data files. URL's are redacted here; contact Sam. 
cd bayarea_urbansim/data
curl -L -k -o 2015_09_01_bayarea_v3.h5 http://full-download-url
curl -L -k -o 2015_06_01_osm_bayarea4326.h5 http://full-download-url
curl -L -k -o 2015_08_03_tmnet.h5 http://full-download-url
curl -L -k -o 2015_12_21_zoning_parcels.csv http://full-download-url
curl -L -k -o 02_01_2016_parcels_geography.csv http://full-download-url
cd ../../

# Run bayarea_urbansim
cd bayarea_urbansim
python run.py

# Display the logfile
cat runs/run1.log
