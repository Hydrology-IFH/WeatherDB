# Change-log

## Version 0.0.6 (upcoming)

## Version 0.0.5
- The et_et0 parameter gor renamed to r_r0 in the create_ts method
- The r_r0 is now possible to add as pd.Serie or list, when creating a timeserie file
- get_meta method of single stations updated
- get_meta for GroupStation(s) updated
- get_df for GroupStation added
- Quickstart added to the documentation
- option to skip the check if a station is in the meta file, this is used for computational advantages in the stations classes, because they test already before creating the objects if they are in the meta table.
- ..._von and ..._bis columns got renamed to the english name ..._from and ..._until
- - the quot_... fields got all normed to % as unit

## Version 0.0.4
- The method part was added to the documentation 
- the connection method got updated

## Version 0.0.3
This is the first released version