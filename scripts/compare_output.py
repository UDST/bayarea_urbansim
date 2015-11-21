from output_csv_utils import *

base_year_df = get_base_year_df()
compare_outcome_for('totemp', runs=[547, 540])
compare_outcome_for('tothh', runs=[547, 540])