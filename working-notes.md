# A working list of items to note during a BAUS run

## Settings

* Profit settings have been lowered to increase development
	* configs\settings.yaml -- cap_rate = 4%
 	* src\urbansim\urbansim\developer\sqftproforma.py -- profit_factor = 5%
 	* src\urbansim\urbansim\developer\sqftproforma.py -- cap_rate = 4%

## Assertions

* Assertions have been disabled in baus\validation.py
	* check_residential_units
	* check_no_overfull_buildings

* An assertion that deed restricted units don't exceed residential units has been disabled
	* baus\subsidies.py -- line 816 in run_subsidized_developer