import os
import re
import numpy as np

# run a full package of scenarios

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

'''
with open('RUNNUM', 'r') as f:
    runnum = f.readline()

lastrun = int(runnum)
lst = [lastrun for i in range(4)]
lst1 = [-5, -4, -3, -2]
runs = [d + c for d, c in zip(lst, lst1)]
os.system('python scripts/compare_output.py "%d" "%d" "%d" "%d"' %
          (runs[0], runs[1], runs[2], runs[3]))
'''
