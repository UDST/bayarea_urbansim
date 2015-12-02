from output_csv_utils import compare_outcome_for
import sys

# compares outcomes from simulation runs
# example usage:
# python scripts/compare_output.py 556 572 611

runs = map(int, sys.argv[1:])

compare_outcome_for('totemp', runs=runs)
compare_outcome_for('tothh', runs=runs)
