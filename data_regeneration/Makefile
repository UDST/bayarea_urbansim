#############################
#############################
#############################
#############################
######Data Regeneration######
#############################
#############################
#############################
#############################
#############################

####THIS HAS NOT BEEN TESTED
get = aws s3 cp s3://landuse/zoning/

#indicators for how well we match the aggregate targets. a good place to compare the targets with what was actually generated.
out/regeneration/summaries/taz_summary.csv: summaries

out/regeneration/summaries/bayarea_v3.h5: load_data \
    load_county_parcel_data_to_staging \
    homogenize_and_clean_county_parcel_attributes \
    merge_county_parcel_tables_into_one_spatial_table \
    spatially_combine_those_parcels \
    assign_historic_sales_data_to_parcels_by_spatial_proximity \
    tag_parcels_with_administrative_names \
    move_jobs_and_houses_to_match_taz_2010_totals_csv \
    predict_prices_of_buildings_and_parcels_with_regressions \
    put_people_jobs_on_parcels \
    summarize_by_taz_and_county \
    export_database_to_h5

demand_agent_allocation: hh/synth/hhFile.p2011s3a1.2010.csv \
    hh/taz2010_imputation.csv
    demand_agent_allocation.py

#print("PREPROCESSING: Loading shapefiles by county.")
load_county_parcel_data_to_staging: /badata/built/parcel/2010/ala/parcelsAlaCo2010/asr_parcel.shp \
    /badata/built/parcel/2010/cnc/raw10/CAD_AO_ParcelPoly_0410.shp \
    /badata/built/parcel/2010/cnc/raw10/CAD_AO_ParcelPoints_int0410.shp \
    /badata/built/parcel/2010/nap/Napa_Parcels.shp \
    /badata/built/parcel/2010/nap/Napa_Census_tract.shp \
    /badata/built/parcel/2005/parcels2005_mar.shp \
    /badata/built/parcel/2010/scl/parcels2010_scl.shp \
    /badata/built/parcel/2010/sfr/parcels2010_sfr.shp \
    /badata/built/parcel/2010/smt/shapefiles/ACTIVE_PARCELS_APN.shp \
    /badata/built/parcel/2010/sol/Parcels.shp \
    /badata/built/parcel/2010/sol/zoning.shp \
    /badata/built/parcel/2010/son/PAR_PARCELS.shp \
    /badata/juris/reg/zones/taz1454.shp \
    /badata/emp/micro/est10_gt1/est10_esri_gt1.shp \
    /badata/built/parcel/2010/rtp13_processing_notes/lucodes.csv
    python load.py

homogenize_and_clean_county_parcel_attributes:
    hca_smt \
    hca_napa \
    hca_scl \
    hca_alameda \
    hca_son \
    hca_sol \
    hca_cnc \
    hca_mar \
    hca_sfr

#following 4 seem to only depend on stuff loaded in staging
#by load_county_parcel_data_to_staging
hca_sol:
    python counties/sol.py

hca_cnc:
    python counties/cnc.py

hca_mar:
    python counties/mar.py

hca_sfr:
    python counties/sfr.py

hca_smt: /badata/built/parcel/2010/smt/Property Characteristics/ASSESSOR_ROLL.mdb \
    /badata/built/parcel/2010/smt/Property Characteristics/SITUS_SNPSHT.csv
    python smt.py

hca_napa: /badata/built/parcel/2010/nap/Napa_buildings.dbf \
    /badata/built/parcel/2010/nap/Napa_taxroll.dbf
    python counties/nap.py

hca_scl: /badata/built/parcel/2010/scl/Scvta031210.dbf
    python counties/scl.py

hca_alameda: /badata/built/parcel/2010/ala/assessor_nov10/IE670c.txt \
    /badata/built/parcel/2010/ala/assessor_nov10/IE673c.txt
    python counties/ala.py

hca_son: /badata/built/parcel/2010/son/ABAG_SonomaCounty.txt
    python counties/son.py

merge_county_parcel_tables_into_one:
    psql join_counties.sql

spatially_combine_those_parcels:
    python geom_aggregation.py

#assigns redfin and costar data to parcels
#this also does various checks for the reasonableness of parcel attributes
#(for example, year built within a reasonable year, price reasonable, etc)
#and then it also seems to make some decisions about when to assign
#the redfin sale price to a parcel
assign_historic_sales_data_to_parcels_by_spatial_proximity: /badata/built/bldg/homeprices/redfin_03feb14.csv \
    /badata/built/bldg/costar/2011/costar__clean2011_sol_020315.csv \
    /badata/built/bldg/costar/2011/costar_allbayarea.xlsx
    python point_based_imputation.py:

tag_parcels_with_administrative_names:
    python spatialops.py

move_jobs_and_houses_to_match_aggregate_taz_2010_totals_csv: /badata/hh/taz2010_imputation.csv \
    /badata/hh/tazsumm_redfin.csv
    python match_aggregate.py

predict_prices_of_buildings_and_parcels_with_regressions: /badata/hh/taz2010_imputation.csv \
    /badata/hh/tazsumm_redfin.csv \
    /badata/hh/synth/hhFile.p2011s3a1.2010.csv
    python price_imputation.py


#also called "demand agent allocation"
put_people_and_jobs_on_parcels: /badata/hh/synth/hhFile.p2011s3a1.2010.csv \
    /badata/hh/taz2010_imputation.csv
    python demand_agent_allocation.py

export_database_to_h5: /badata/hh/reg/zones/zones.csv
#output=/badata/out/regeneration/summaries/bayarea_v2.h5
    python export_to_h5.py





# Load TAZ residential unit control totals.
summarize_by_taz_and_county: /badata/hh/taz2010_imputation.csv \
    /badata/out/regeneration/summaries/buildings.csv \
    /badata/out/regeneration/summaries/taz_summary.csv
    python summaries.py

/badata/built/parcel/2010/ala/parcelsAlaCo2010/asr_parcel.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/cnc/raw10/CAD_AO_ParcelPoly_0410.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/cnc/raw10/CAD_AO_ParcelPoints_int0410.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/nap/Napa_Parcels.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/nap/Napa_Census_tract.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2005/parcels2005_mar.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/scl/parcels2010_scl.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/sfr/parcels2010_sfr.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/smt/shapefiles/ACTIVE_PARCELS_APN.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/sol/Parcels.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/sol/zoning.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/son/PAR_PARCELS.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/juris/reg/zones/taz1454.shp:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/emp/micro/est10_gt1/est10_esri_gt1.sh:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/rtp13_processing_notes/lucodes.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/juris/loc/zoning/zoning_parcels.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/juris/loc/zoning/zoning_id_lookup_table.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/hh/synth/hhFile.p2011s3a1.2010.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/hh/taz2010_imputation.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/juris/reg/zones/zones.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/hh/taz2010_imputation.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/hh/tazsumm_redfin.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/bldg/homeprices/redfin_03feb14.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/bldg/costar/2011/costar__clean2011_sol_020315.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/bldg/costar/2011/costar_allbayarea.xlsx:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/hh/taz2010_imputation.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/hh/tazsumm_redfin.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/hh/synth/hhFile.p2011s3a1.2010.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/hh/taz2010_imputation.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/out/regeneration/summaries/buildings.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/out/regeneration/summaries/taz_summary.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/smt/Property Characteristics/ASSESSOR_ROLL.mdb:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/smt/Property Characteristics/SITUS_SNPSHT.csv:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/nap/Napa_buildings.dbf:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/nap/Napa_taxroll.dbf:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/scl/Scvta031210.dbf:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/ala/assessor_nov10/IE670c.txt:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/ala/assessor_nov10/IE673c.tx:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@

/badata/built/parcel/2010/son/ABAG_SonomaCounty.txt:
    $(get)$@ \
    $@.download
    mv $@.download $@
    touch $@