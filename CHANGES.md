# Change-log

# Version 0.0.8
- fixed installation (psycopg2 problem and DB_ENG creation)
- fixed imnporting module when not super user

## Version 0.0.7
- convert timezone of downloaded precipitation data, because (before 200 the data is in "MEZ" afterwards in "UTC")
- update_ma: 
  - Rasters now also have proj4 code, if necessary. Because the postgis database is not supporting transformation to EPSG:31467 
  - small speed improvement
- StationCanVirtual._check_meta updated to check separately if station is in meta table and if it has a timeseries table
- Added timezone support. The database timezone is UTC.

## Version 0.0.6
- error fixed with is_virtual (!important error!)
- human readable format for the period in log message added
- some spelling errors fixed in documentation
- kwargs added to child methods of get_df (like get_raw...)
- in get_df and consecutive methods: 
  - filled_share column added if aggregating and filled_by selected
  - possibility to download filled_by added
  - nas_allowed option added
  - add_na_share option added. (give the share of NAs if aggregating) 
- in create_ts option to save several kinds added
- get_max_period method
- error in check_stids fixed
- error in ma_update fixed

## Version 0.0.5
- The et_et0 parameter gor renamed to r_r0 in the create_ts method
- The r_r0 is now possible to add as pd.Serie or list, when creating a timeserie file
- get_meta method of single stations updated
- get_meta for GroupStation(s) updated
- get_df for GroupStation added
- Quickstart added to the documentation
- documentation has now a TOC tree per class and a method TOC tree on top
- option to skip the check if a station is in the meta file, this is used for computational advantages in the stations classes, because they test already before creating the objects if they are in the meta table.
- ..._von and ..._bis columns got renamed to the english name ..._from and ..._until
- the quot_... fields got all normed to % as unit
- dropping stations from meta while updating checks now if stid is in downloaded meta file

## Version 0.0.4
- The method part was added to the documentation 
- the connection method got updated

## Version 0.0.3
This is the first released version