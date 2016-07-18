import os
import re
import numpy as np

# this script will probably eventually go away - it is for exploring what
# happens as we vary the combination of profit to probability functions from
# 100% return on cost (the status quo) to slowly mixing back in the raw profit
# which is how this used to be done.  In fact the user has complete control
# over what function might be used here - this is just one example.  In this
# case we're running 1.0 * return on cost + X * raw profit where X varies from
# 0 to 1 in steps of .05.  This script opens up the yaml subs out the one
# parameter that needs to fixed and runs run.py

for factor in np.arange(0, 1.01, .05):

    fname = "configs/settings.yaml"

    s = open(fname).read()

    s = re.sub(
		"profit_vs_return_on_cost_combination_factor: .*\n",
		"profit_vs_return_on_cost_combination_factor: %f\n" % factor,
		s
	)
    open(fname, "w").write(s)

    os.system('python run.py -s 4')
