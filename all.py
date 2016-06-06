import os

# run a full package of scenarios

for num in [0, 1, 2, 3, 4]:
    os.system('python run.py -s %d' % num)

with open('RUNNUM', 'r') as f:
    runnum = f.readline()

lastrun = int(runnum)
lst = [lastrun for i in range(4)]
lst1 = [-5, -4, -3, -2]
runs = [d + c for d, c in zip(lst, lst1)]
os.system('python scripts/compare_output.py "%d" "%d" "%d" "%d"' %
          (runs[0], runs[1], runs[2], runs[3]))
