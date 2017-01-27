import orca

# the way this works is there is an orca step to do jobs allocation, which
# reads base year totals and creates jobs and allocates them to buildings,
# and writes it back to the h5.  then the actual jobs table above just reads
# the auto-allocated version from the h5.  was hoping to just do allocation
# on the fly but it takes about 4 minutes so way to long to do on the fly


@orca.step()
def allocate_jobs(store, baseyear_taz_controls, settings, parcels):

    # this isn't pretty, but can't use orca table because there would
    # be a circular dependenct - I mean jobs dependent on buildings and
    # buildings on jobs, so we have to grab from the store directly
    buildings = store['buildings']
    buildings["non_residential_sqft"][
        buildings.building_type_id.isin([15, 16])] = 0
    buildings["building_sqft"][buildings.building_type_id.isin([15, 16])] = 0
    buildings["zone_id"] = misc.reindex(parcels.zone_id, buildings.parcel_id)

    # we need to do a new assignment from the controls to the buildings

    # first disaggregate the job totals
    sector_map = settings["naics_to_empsix"]
    jobs = []
    for taz, row in baseyear_taz_controls.local.iterrows():
        for sector_col, num in row.iteritems():

            # not a sector total
            if not sector_col.startswith("emp_sec"):
                continue

            # get integer sector id
            sector_id = int(''.join(c for c in sector_col if c.isdigit()))
            sector_name = sector_map[sector_id]

            jobs += [[sector_id, sector_name, taz, -1]] * num

    # df is now the
    df = pd.DataFrame(jobs, columns=[
        'sector_id', 'empsix', 'taz', 'building_id'])

    # just do random assignment weighted by job spaces - we'll then
    # fill in the job_spaces if overfilled in the next step (code
    # has existed in urbansim for a while)
    for taz, cnt in df.groupby('taz').size().iteritems():

        potential_add_locations = buildings.non_residential_sqft[
            (buildings.zone_id == taz) &
            (buildings.non_residential_sqft > 0)]

        if len(potential_add_locations) == 0:
            # if no non-res buildings, put jobs in res buildings
            potential_add_locations = buildings.building_sqft[
                buildings.zone_id == taz]

        weights = potential_add_locations / potential_add_locations.sum()

        print taz, len(potential_add_locations),\
            potential_add_locations.sum(), cnt

        buildings_ids = potential_add_locations.sample(
            cnt, replace=True, weights=weights)

        df["building_id"][df.taz == taz] = buildings_ids.index.values

    s = buildings.zone_id.loc[df.building_id].value_counts()
    t = baseyear_taz_controls.emp_tot - s
    # assert we matched the totals exactly
    assert t.sum() == 0

    store['jobs_urbansim_allocated'] = df
