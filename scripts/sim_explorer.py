from urbansim_explorer import sim_explorer as se
import sys
run_num = int(sys.argv[1])
se.start(
    'runs/run%d_simulation_output.json' % run_num,
    'runs/run%d_parcel_output.csv' % run_num,
    write_static_file='/var/www/html/sim_explorer%d.html' % run_num
)
