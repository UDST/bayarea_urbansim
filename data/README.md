A few notes on the zoning_lookup, which hopefully makes this an evolving document.

The zoning lookup is the current, BASELINE zoning for each jurisdiction, and can be assigned to parcels.  The lookup is identified by the unique id ("id" column), has the city name, city id, and the name of the zoning.

The active attributes are max_dua, max_far, and max_height, all of which must be respected by each development, so this means the most constraining constraint has the power.  dua is "Dwelling Units per Acre", far is "Floor Area Ratio" (ratio of square footage in building to square footage on parcel), and height is... height. 

This means there *must* be an equivlance between dua and far if they are both present.  This is tricky, and requires knowing things like average unit sizes, as well as net to gross factors for the unit square footage.  We make our best guesses on this, but this logic can be configured.  There also must be an equivalance between far and height, which generally is made by taking far and multiplying is by a net to gross building footprint factor (e.g. a calculation to figure out the height of a building with FAR 4.0, when the footprint of the building takes up 80% or so of the parcel, when stories are all 12 feet, and where partial stories count as full stories in terms of height).  As you are thinking at this point, we are bad building architects, but we're making rough approximations here in order to allocate built space in the region and this feels like the appropriate level of detail.

The other columns are are building types, and the values are 0 or 1 to indicate whether the type is allowed on this parcel.  For instance, HS is single family, HT is townhome (attached), and HM is multi-family (apartments / condos) and so forth.  R for retail, OF is office, and M is mixed.

max_du_per_parcel is not currently used because we don't necessarily trust our data, but in theory is used to say, e.g., that only a single unit can be built on each parcel.

Note that a blank value in the csv means that there is no constraint for that attribute and the other constraints will be used.  If there is no constraint for any of far, height, and dua, a building will NOT be built. 

*By convention a 0 in dua, far, and height is discouraged.  It is preferred to use the "building type not allowed" columns for this*
