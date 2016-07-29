from output_csv_utils import compare_outcome_for
import sys

# compares outcomes from simulation runs
# example usage:
# python scripts/compare_output.py 556 572 611

runs = map(int, sys.argv[1:])

#compare_outcome_for('tothh', set_geography='pda', runs=runs)
#compare_outcome_for('totemp', set_geography='pda', runs=runs)
compare_outcome_for('tothh', set_geography='juris', runs=runs)
compare_outcome_for('totemp', set_geography='juris', runs=runs)
#compare_outcome_for('tothh', set_geography='superdistrict', runs=runs)
#compare_outcome_for('totemp', set_geography='superdistrict', runs=runs)
#compare_outcome_for('TOTHH', set_geography='taz', runs=runs)
#compare_outcome_for('TOTEMP', set_geography='taz', runs=runs)