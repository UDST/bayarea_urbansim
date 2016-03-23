from urbansim_explorer import sim_explorer as se
import sys

runnum = int(sys.argv[1])

parcel_output = 'runs/run%d_parcel_output.csv' % runnum
zone_output = 'runs/run%d_simulation_output.json' % runnum
outfile = '/var/www/html/sim_explorer%d.html' % runnum

se.start(
    zone_output,
    parcel_output,
    port=8080,
    host='0.0.0.0',
    write_static_file=outfile
)
