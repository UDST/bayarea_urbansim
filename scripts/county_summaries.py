import pandas as pd
from pandas import ExcelWriter

counties_numbers_to_names = {
  3: "Santa Clara",
  4: "Alameda",
  5: "Contra Costa",
  2: "San Mateo",
  8: "Sonoma",
  1: "San Francisco",
  6: "Solano",
  9: "Marin",
  7: "Napa"
}

counties_map = pd.read_csv("data/taz_geography.csv", index_col="zone").\
	county.map(counties_numbers_to_names)

writer = ExcelWriter('county_output.xlsx')

parcels_to_counties = pd.HDFStore("data/2015_09_01_bayarea_v3.h5", "r").\
	parcels.zone_id.map(counties_map)

for run in range(1308, 1312):

	df = pd.read_csv("http://urbanforecast.com/runs/"\
		"run%d_parcel_output.csv" % run)
	df["county"] = df.parcel_id.map(parcels_to_counties)
	growthinpdas = df[(df.building_type_id <= 3) & (df.pda.notnull())].\
	    groupby("county").net_units.sum()
	growthnotinpdas = df[(df.building_type_id <= 3) & (df.pda.isnull())].\
	    groupby("county").net_units.sum()
	pctgrowthinpdas = growthinpdas / (growthnotinpdas+growthinpdas)
	print pctgrowthinpdas

	baseyear = pd.read_csv("output/baseyear_taz_summaries_2010.csv")
	baseyear["county"] = baseyear.zone_id.map(counties_map)

	outyear = pd.read_csv("http://urbanforecast.com/runs/"\
		"run%d_taz_summaries_2040.csv" % run)
	outyear["county"] = outyear.zone_id.map(counties_map)

	hhpctgrowth = outyear.groupby("county").TOTPOP.sum() / \
		baseyear.groupby("county").TOTPOP.sum() - 1

	s = outyear.groupby("county").TOTPOP.sum() - \
		baseyear.groupby("county").TOTPOP.sum()
	hhgrowthshare = s / s.sum()

	emppctgrowth = outyear.groupby("county").TOTEMP.sum() / \
		baseyear.groupby("county").TOTEMP.sum() - 1

	s = outyear.groupby("county").TOTEMP.sum() - \
		baseyear.groupby("county").TOTEMP.sum()
	empgrowthshare = s / s.sum()

	growthinunits = outyear.eval("SFDU + MFDU").groupby(outyear.county).sum() - \
		baseyear.eval("SFDU + MFDU").groupby(baseyear.county).sum()

	growthinmultifamily = outyear.groupby(outyear.county).MFDU.sum() - \
		baseyear.groupby(baseyear.county).MFDU.sum()
	pct_multifamily_growth = growthinmultifamily / growthinunits

	df = pd.DataFrame({
		"pct_growth_in_pdas": pctgrowthinpdas,
		"hh_pct_growth": hhpctgrowth,
		"hh_growth_share": hhgrowthshare,
		"emp_pct_growth": emppctgrowth,
		"emp_growth_share": empgrowthshare,
		"growth_in_units": growthinunits.astype('int'),
		"pct_multifamily_growth": pct_multifamily_growth.clip(upper=1.0)
	})

	df.index.name = None

	df.to_excel(writer, 'run%d' % run, float_format="%.2f")

