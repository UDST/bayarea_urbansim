import pandas as pd
import numpy as np
import orca
import os
from urbansim_defaults.utils import _remove_developed_buildings
from urbansim.developer.developer import Developer as dev


#####################
# UTILITY FUNCTIONS
#####################


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


# pick random indexes from s without replacement
def random_indexes(s, num):
    return np.random.choice(s.index.values, int(num), replace=False)


# This method takes a series of floating point numbers, rounds to
# integers (e.g. to while number households), while making sure to
# meet the given target for the sum.  We're obviously going to lose
# some resolution on the distrbution implied by s in order to meet
# the target exactly
def round_series_match_target(s, target, fillna):
    s = s.fillna(fillna).round().astype('int')
    diff = target - s.sum()
    if diff > 0:
        s.loc[random_indexes(s, diff)] += 1
    elif diff < 0:
        s.loc[random_indexes(s, diff*-1)] -= 1

    assert s.sum() == target
    return s


# scales (floating point ok) so that the sum of s if equal to
# the specified target - pass check_close to verify that it's
# within a certain range of the target
def scale_by_target(s, target, check_close=None):
    ratio = float(target) / s.sum()
    if check_close:
        assert 1.0-check_close < ratio < 1.0+check_close
    return s * ratio


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
    # print "row closeness", closeness
    if closeness < tolerance:
        return seed_matrix

    # first normalize on rows
    ratios = row_marginals / seed_matrix.sum(axis=1)
    ratios[row_marginals == 0] = 0
    seed_matrix = seed_matrix * ratios.reshape((ratios.size, 1))
    assert np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum() < .01
    closeness = np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum()
    # print "col closeness", closeness
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
