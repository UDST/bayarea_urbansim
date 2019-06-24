import pandas as pd
import numpy as np
import re
import orca
import os
import sys
from urbansim_defaults.utils import _remove_developed_buildings
from urbansim.developer.developer import Developer as dev
from urbansim.developer import sqftproforma
from urbansim.utils import misc


#####################
# UTILITY FUNCTIONS
#####################


# This is really simple and basic (hackish) code to solve a very important
# problem.  I'm calling an orca step and want to serialize the tables that
# are passed in so that I can add code to this orca step and test it quickly
# without running all the models that came before, which takes many minutes.
#
# If the passed in outhdf does not exist, all the DataFarmeWrappers which
# have been passed have their DataFrames saved to the hdf5.  If the file
# does exist they all get restored, and as a byproduct their types are
# changed from DataFrameWrappers to DataFrames.  This type change is a
# major drawback of the current code, as is the fact that other non-
# DataFrame types are not serialized, but it's a start.  I plan to bring
# this problem up on the UrbanSim coders call the next time we meet.
#
#    Sample code:
#
#    d = save_and_restore_state(locals())
#    for k in d.keys():
#        locals()[k].local = d[k]
#
def save_and_restore_state(in_d, outhdf="save_state.h5"):
    if os.path.exists(outhdf):
        # the state exists - load it back into the locals
        store = pd.HDFStore(outhdf)
        out_d = {}
        for table_name in store:
            print "Restoring", table_name
            out_d[table_name[1:]] = store[table_name]
        return out_d

    # the state doesn't exist and thus needs to be saved
    store = pd.HDFStore(outhdf, "w")
    for table_name, table in in_d.items():
        try:
            table = table.local
        except:
            # not a dataframe wrapper
            continue
        print "Saving", table_name
        store[table_name] = table
    store.close()
    sys.exit(0)


# similar to the function in urbansim_defaults, except it assumes you want
# to use your own pick function
def add_buildings(buildings, new_buildings,
                  remove_developed_buildings=True):

    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = new_buildings[buildings.local_columns]

    if remove_developed_buildings:
        unplace_agents = ["households", "jobs"]
        old_buildings = \
            _remove_developed_buildings(old_buildings, new_buildings,
                                        unplace_agents)

    all_buildings = dev.merge(old_buildings, new_buildings)

    orca.add_table("buildings", all_buildings)


# assume df1 and df2 each have 2 float columns specifying x and y
# in the same order and coordinate system and no nans.  returns the indexes
# from df1 that are closest to each row in df2
def nearest_neighbor(df1, df2):
    from sklearn.neighbors import KDTree
    kdt = KDTree(df1.as_matrix())
    indexes = kdt.query(df2.as_matrix(), k=1, return_distance=False)
    return df1.index.values[indexes]


# need to reindex from geom id to the id used on parcels
def geom_id_to_parcel_id(df, parcels):
    s = parcels.geom_id  # get geom_id
    s = pd.Series(s.index, index=s.values)  # invert series
    df["new_index"] = s.loc[df.index]  # get right parcel_id for each geom_id
    df = df.dropna(subset=["new_index"])
    df["new_index"] = df.new_index.astype('int')
    df = df.set_index("new_index", drop=True)
    df.index.name = "parcel_id"
    return df


def parcel_id_to_geom_id(s):
    parcels = orca.get_table("parcels")
    g = parcels.geom_id  # get geom_id
    return pd.Series(g.loc[s.values].values, index=s.index)


# This is best described by example. Imagine s is a series where the
# index is parcel ids and the values are cities, while counts is a
# series where the index is cities and the values are counts.  You
# want to end up with "counts" many parcel ids from s (like 40 from
# Oakland, 60 from SF, and 20 from San Jose).  This can be
# thought of as grouping the dataframe "s" came from and sampling
# count number of rows from each group.  I mean, you group the
# dataframe and then counts gives you the count you want to sample
# from each group.
def groupby_random_choice(s, counts, replace=True):
    if counts.sum() == 0:
        return pd.Series()

    return pd.concat([
        s[s == grp].sample(cnt, replace=replace)
        for grp, cnt in counts[counts > 0].iteritems()
    ])


# pick random indexes from s without replacement
def random_indexes(s, num, replace=False):
    return np.random.choice(
        np.repeat(s.index.values, s.values),
        num,
        replace=replace)


# This method takes a series of floating point numbers, rounds to
# integers (e.g. to while number households), while making sure to
# meet the given target for the sum.  We're obviously going to lose
# some resolution on the distrbution implied by s in order to meet
# the target exactly
def round_series_match_target(s, target, fillna=np.nan):
    if target == 0 or s.sum() == 0:
        return s
    r = s.fillna(fillna).round().astype('int')
    # handles rare cases where all values round to 0
    if r.sum() == 0:
        r = np.ceil(s).astype('int')
    diff = int(np.subtract(target, r.sum()))
    # diff = int(target - r.sum())
    if diff > 0:
        # replace=True allows us to add even more than we have now
        indexes = random_indexes(r, abs(diff), replace=True)
        r = r.add(pd.Series(indexes).value_counts(), fill_value=0)
    elif diff < 0:
        # this makes sure we can only subtract until all rows are zero
        indexes = random_indexes(r, abs(diff))
        r = r.sub(pd.Series(indexes).value_counts(), fill_value=0)

    assert r.sum() == target
    return r


# scales (floating point ok) so that the sum of s if equal to
# the specified target - pass check_close to verify that it's
# within a certain range of the target
def scale_by_target(s, target, check_close=None):
    ratio = float(target) / s.sum()
    if check_close:
        assert 1.0-check_close < ratio < 1.0+check_close
    return s * ratio


def constrained_normalization(marginals, constraint, total):
    # this method increases the marginals to match the total while
    # also meeting the matching constraint.  marginals should be
    # scaled up proportionally.  it is possible that this method
    # will fail if the sum of the constraint is less than the total

    assert constraint.sum() >= total

    while 1:

        # where do we exceed the total
        constrained = marginals >= constraint
        exceeds = marginals > constraint
        unconstrained = ~constrained

        num_constrained = len(constrained[constrained is True])
        num_exceeds = len(exceeds[exceeds is True])

        print "Len constrained = %d, exceeds = %d" %\
            (num_constrained, num_exceeds)

        if num_exceeds == 0:
            return marginals

        marginals[constrained] = constraint[constrained]

        # scale up where unconstrained
        unconstrained_total = total - marginals[constrained].sum()
        marginals[unconstrained] *= \
            unconstrained_total / marginals[unconstrained].sum()

        # should have scaled up
        assert np.isclose(marginals.sum(), total)


# this should be fairly self explanitory if you know ipf
# seed_matrix is your best bet at the totals, col_marginals are
# observed column marginals and row_marginals is the same for rows
def simple_ipf(seed_matrix, col_marginals, row_marginals, tolerance=1, cnt=0):
    assert np.absolute(row_marginals.sum() - col_marginals.sum()) < 5.0

    # first normalize on columns
    ratios = col_marginals / seed_matrix.sum(axis=0)
    seed_matrix *= ratios
    closeness = np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum()
    assert np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum() < .01
    print "row closeness", closeness
    if closeness < tolerance:
        return seed_matrix

    # first normalize on rows
    ratios = np.array(row_marginals / seed_matrix.sum(axis=1))
    ratios[row_marginals == 0] = 0
    seed_matrix = seed_matrix * ratios.reshape((ratios.size, 1))
    assert np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum() < .01
    closeness = np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum()
    print "col closeness", closeness
    if closeness < tolerance:
        return seed_matrix

    if cnt >= 50:
        return seed_matrix

    return simple_ipf(seed_matrix, col_marginals, row_marginals,
                      tolerance, cnt+1)


"""
BELOW IS A SET OF UTITLIES TO COMPARE TWO SUMMARY DATAFRAMES, MAINLY LOOKING
FOR PCT DIFFERENCE AND THEN FORMATTING INTO A DESCRIPTION OR INTO AN EXCEL
OUTPUT FILE WHICH IS COLOR CODED TO HIGHLIGHT THE DIFFERENCES
"""

# for labels and cols in df1, find value in df2, and make sure value is
# within pctdiff - if not return dataframe col, row and values in two frames
# pctdiff should be specified as a number between 1 and 100


def compare_dfs(df1, df2):

    df3 = pd.DataFrame(index=df1.index, columns=df1.columns)

    # for each row
    for label, row in df1.iterrows():

        # assume row exists in comparison
        rowcomp = df2.loc[label]

        # for each value
        for col, val in row.iteritems():

            val2 = rowcomp[col]

            df3.loc[label, col] = \
                int(abs(val - val2) / ((val + val2 + .01) / 2.0) * 100.0)

    return df3


# identify small values as a boolean T/F for each column
def small_vals(df):

    df = df.copy()

    for col in df.columns:
        df[col] = df[col] < df[col].mean() - .5*df[col].std()

    return df


def compare_dfs_excel(df1, df2, excelname="out.xlsx"):
    import palettable
    import xlsxwriter

    writer = pd.ExcelWriter(excelname, engine='xlsxwriter')

    df1.reset_index().to_excel(writer, index=False, sheet_name='df1')
    df2.reset_index().to_excel(writer, index=False, sheet_name='df2')
    writer.sheets['df1'].set_zoom(150)
    writer.sheets['df2'].set_zoom(150)

    df3 = compare_dfs(df1, df2)

    df3.reset_index().to_excel(writer, index=False, sheet_name='comparison')

    workbook = writer.book
    worksheet = writer.sheets['comparison']
    worksheet.set_zoom(150)

    color_range = "B2:Z{}".format(len(df1)+1)

    reds = palettable.colorbrewer.sequential.Blues_5.hex_colors
    reds = {
        i: workbook.add_format({'bg_color': reds[i]})
        for i in range(len(reds))
    }

    blues = palettable.colorbrewer.sequential.Oranges_5.hex_colors
    blues = {
        i: workbook.add_format({'bg_color': blues[i]})
        for i in range(len(blues))
    }

    def apply_format(worksheet, df, format, filter):

        s = df.stack()

        for (lab, col), val in filter(s).iteritems():

            rowind = df.index.get_loc(lab)+2
            colind = df.columns.get_loc(col)+1
            colletter = xlsxwriter.utility.xl_col_to_name(colind)

            worksheet.write(colletter+str(rowind), val, format)

    for i in range(5):
        apply_format(worksheet, df3, reds[i], lambda s: s[s > i*10+10])

    df3[small_vals(df1)] = np.nan
    for i in range(5):
        apply_format(worksheet, df3, blues[i], lambda s: s[s > i*10+10])

    writer.save()


# compare certain columns of two dataframes for differences above a certain
# amount and return a string describing the differences
def compare_summary(df1, df2, index_names=None, pctdiff=10,
                    cols=["tothh", "totemp"], geog_name="Superdistrict"):

    if cols:
        df1, df2 = df1[cols], df2[cols]

    df3 = compare_dfs(df1, df2)
    df3[small_vals(df1)] = np.nan
    s = df3[cols].stack()

    buf = ""
    for (lab, col), val in s[s > 10].iteritems():
        lab = index_names.loc[lab]
        buf += "%s '%s' is %d%% off in column '%s'\n" % \
            (geog_name, lab, val, col)

    return buf


# copied from urbansim_defaults but only using local cols for parcels table
def run_feasibility(parcels, parcel_price_callback,
                    parcel_use_allowed_callback, residential_to_yearly=True,
                    parcel_filter=None, only_built=True, forms_to_test=None,
                    config=None, pass_through=[], simple_zoning=False):
    """
    Execute development feasibility on all parcels
    Parameters
    ----------
    parcels : DataFrame Wrapper
        The data frame wrapper for the parcel data
    parcel_price_callback : function
        A callback which takes each use of the pro forma and returns a series
        with index as parcel_id and value as yearly_rent
    parcel_use_allowed_callback : function
        A callback which takes each form of the pro forma and returns a series
        with index as parcel_id and value and boolean whether the form
        is allowed on the parcel
    residential_to_yearly : boolean (default true)
        Whether to use the cap rate to convert the residential price from total
        sales price per sqft to rent per sqft
    parcel_filter : string
        A filter to apply to the parcels data frame to remove parcels from
        consideration - is typically used to remove parcels with buildings
        older than a certain date for historical preservation, but is
        generally useful
    only_built : boolean
        Only return those buildings that are profitable - only those buildings
        that "will be built"
    forms_to_test : list of strings (optional)
        Pass the list of the names of forms to test for feasibility - if set to
        None will use all the forms available in ProFormaConfig
    config : SqFtProFormaConfig configuration object.  Optional.  Defaults to
        None
    pass_through : list of strings
        Will be passed to the feasibility lookup function - is used to pass
        variables from the parcel dataframe to the output dataframe, usually
        for debugging
    simple_zoning: boolean, optional
        This can be set to use only max_dua for residential and max_far for
        non-residential.  This can be handy if you want to deal with zoning
        outside of the developer model.
    Returns
    -------
    Adds a table called feasibility to the sim object (returns nothing)
    """

    pf = sqftproforma.SqFtProForma(config) if config \
        else sqftproforma.SqFtProForma()
    filter_list = re.split('and|or', parcel_filter)
    filter_columns = [
        x.strip().replace(")", '').replace("(", '').split()[
            0] for x in filter_list]
    foreign_columns = list(set(filter_columns + pass_through))

    # see line 572 in urbansim.developer.sqftproforma
    # both columns come from urbansim_defaults.variables
    if 'max_dua' in foreign_columns:
        foreign_columns += ['ave_unit_size', 'parcel_size']

    df = parcels.to_frame(
        parcels.local_columns + foreign_columns)

    if parcel_filter:
        df = df.query(parcel_filter)

    # add prices for each use
    for use in pf.config.uses:
        # assume we can get the 80th percentile price for new development
        df[use] = parcel_price_callback(use)

    # convert from cost to yearly rent
    if residential_to_yearly:
        df["residential"] *= pf.config.cap_rate

    print "Describe of the yearly rent by use"
    print df[pf.config.uses].describe()

    d = {}
    forms = forms_to_test or pf.config.forms
    for form in forms:
        print "Computing feasibility for form %s" % form
        allowed = parcel_use_allowed_callback(form).loc[df.index]

        newdf = df[allowed]
        if simple_zoning:
            if form == "residential":
                # these are new computed in the effective max_dua method
                newdf["max_far"] = pd.Series()
                newdf["max_height"] = pd.Series()
            else:
                # these are new computed in the effective max_far method
                newdf["max_dua"] = pd.Series()
                newdf["max_height"] = pd.Series()

        d[form] = pf.lookup(form, newdf, only_built=only_built,
                            pass_through=pass_through)
        if residential_to_yearly and "residential" in pass_through:
            d[form]["residential"] /= pf.config.cap_rate

    far_predictions = pd.concat(d.values(), keys=d.keys(), axis=1)

    orca.add_table("feasibility", far_predictions)


def register_skim_access_variable(
        column_name, variable_to_summarize, impedance_measure,
        distance, log=False):
    """
    Register skim-based accessibility variable with orca.
    Parameters
    ----------
    column_name : str
        Name of the orca column to register this variable as.
    impedance_measure : str
        Name of the skims column to use to measure inter-zone impedance.
    variable_to_summarize : str
        Name of the zonal variable to summarize.
    distance : int
        Distance to query in the skims (e.g. 30 minutes travel time).
    Returns
    -------
    column_func : function
    """
    @orca.column('zones', column_name, cache=True, cache_scope='iteration')
    def column_func(zones, beam_skims_imputed):
        results = misc.compute_range(
            beam_skims_imputed.to_frame(),
            zones.get_column(variable_to_summarize),
            impedance_measure, distance, agg=np.sum)

        if log:
            results = results.apply(eval('np.log1p'))

        if len(results) < len(zones):
            results = results.reindex(zones.index).fillna(0)

        return results
    return column_func


def impute_missing_skims(mtc_skims, beam_skims_raw):

    df = beam_skims_raw.to_frame()

    # seconds to minutes
    df['gen_tt'] = df['generalizedTimeInS'] / 60

    mtc = mtc_skims.to_frame(columns=['orig', 'dest', 'da_distance_AM'])
    mtc.rename(
        columns={'orig': 'from_zone_id', 'dest': 'to_zone_id'},
        inplace=True)
    mtc.set_index(['from_zone_id', 'to_zone_id'], inplace=True)

    # miles to meters
    mtc['dist'] = mtc['da_distance_AM'] * 1609.34

    # create morning peak lookup
    df['gen_time_per_m'] = df['gen_tt'] / df['distanceInM']
    df['gen_cost_per_m'] = df['gen_cost'] / df['distanceInM']
    df.loc[df['hour'].isin([7, 8, 9]), 'period'] = 'AM'
    df_am = df[df['period'] == 'AM']
    df_am = df_am.replace([np.inf, -np.inf], np.nan)
    am_lookup = df_am[[
        'mode', 'gen_time_per_m', 'gen_cost_per_m']].dropna().groupby(
            ['mode']).mean().reset_index()

    # morning averages
    df_am_avg = df_am[[
        'from_zone_id', 'to_zone_id', 'mode', 'gen_tt',
        'gen_cost']].groupby(
        ['from_zone_id', 'to_zone_id', 'mode']).mean().reset_index()

    # long to wide
    df_am_pivot = df_am_avg.pivot_table(
        index=['from_zone_id', 'to_zone_id'], columns='mode')
    df_am_pivot.columns = ['_'.join(col) for col in df_am_pivot.columns.values]

    # combine with mtc-based dists
    merged = pd.merge(
        mtc[['dist']], df_am_pivot, left_index=True, right_index=True,
        how='left')

    # impute
    for mode in am_lookup['mode'].values:
        for impedance in ['gen_tt', 'gen_cost']:
            if impedance == 'gen_tt':
                lookup_col = 'gen_time_per_m'
            elif impedance == 'gen_cost':
                lookup_col = 'gen_cost_per_m'
            colname = impedance + '_' + mode
            lookup_val = am_lookup.loc[
                am_lookup['mode'] == mode, lookup_col].values[0]
            merged.loc[pd.isnull(merged[colname]), colname] = merged.loc[
                pd.isnull(merged[colname]), 'dist'] * lookup_val

    assert len(merged) == 2114116

    return merged
