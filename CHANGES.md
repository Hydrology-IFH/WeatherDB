# Change-log

## Version 0.0.28
- MAJOR Error fix: The quality check for T and ET did not consider the decimal multiplier for the limits. So the table 2 from the Method documentation should have looked like this until now, in bold are the numbers that were wrong in the code:

| parameter | compare equation | lower limit | upper limit|
|:---:|:---:|:---:|:---:|
| Temperature |  $\Delta T = T_{Stat} - \overline{T}_{neighbors}$ | $\Delta T < -\bm{0.5}°C$ | $\Delta T > \bm{0.5}°C$ |
| pot. Evapotranspiration |  $\delta ET = \dfrac{ET_{Stat}}{\overline{ET}_{neighbors}}$ | $\begin{cases}\delta ET< \bm{20}\% \\ ET_{Stat}> \bm{0.2} \frac{mm}{d}\end{cases}$|$\begin{cases}\delta ET> 200\% \\ ET_{Stat}> \bm{0.3} \frac{mm}{d}\end{cases}$|

Those limits got corrected to correspond now to:
| parameter | compare equation | lower limit | upper limit|
|:---:|:---:|:---:|:---:|
| Temperature |  $\Delta T = T_{Stat} - \overline{T}_{neighbors}$ | $\Delta T < -\bm{5}°C$ | $\Delta T > \bm{5}°C$ |
| pot. Evapotranspiration |  $\delta ET = \dfrac{ET_{Stat}}{\overline{ET}_{neighbors}}$ | $\begin{cases}\delta ET< \bm{25}\% \\ ET_{Stat}> \bm{2} \frac{mm}{d}\end{cases}$|$\begin{cases}\delta ET> 200\% \\ ET_{Stat}> \bm{3} \frac{mm}{d}\end{cases}$|
- fixed error that came up in version 0.0.27 for richter correction. The horizon was only calculated from west to south not from north to south.

## Version 0.0.27
- fixed major error with update_horizon method. Therefor the Richter Exposition classe changes for many stations. This error existed since Version 0.0.15
- add multiprocess ability to update_richter_class 

## Version 0.0.26
- fix error with sql statements
- fix logging
## Version 0.0.25
**version has major problems, use version 0.0.26**
- change logging.py submodule name, because of import conflicts with python logging package
## Version 0.0.24
- add text wrapper from sqlalchemy to work with sqlalchemy version >2.0
- add compatibility for shapely >2.0
## Version 0.0.23
- change pandas to_csv parameter line_terminator to lineterminator, for newer versions
- change logging procedure, to not log to file as a standard way, but only after calling setup_file_logging from logging.py
## Version 0.0.22
- add qc_from and qc_until to the meta informations
- fix removal of old log files
## Version 0.0.21
- add additional parameter sql_add_where to define a sql where statement to filter the created results in the database
- add postgresql error messages that will cause the execution to wait and restart
- import Station(s)-classes imediatly when module is imported, so now this works 
  ```
  import weatherDB as wdb
  wdb.StationsN()
  ```
## Version 0.0.20
- change secretSettings_weatherDB names to DB_PWD, DB_NAME and DB_USER
- add min and max to the temperature timeseries

## Version 0.0.19
- fix error of updating raw_files table after new import.
- change log file name to weatherDB_%host%_%user%.log
- change the use of append method to pandas concat method
- changed pandas method iteritems to items, due to deprecation warning

## Version 0.0.18
- correct spelling error "methode" to "method"
- add progressbar to count_holes method
- add para to raw_files db-table, because some files get used for several parameters (T and N_D)

## Version 0.0.17
- get_df now also accepts filled_share as kind
- added function to count the holes in the timeseries depending on there length

## Version 0.0.16
- repaired the update_raw function of StationND
- change data source from REGNIE to HYRAS for precipitation regionalisation
- add ability to get nearby ma value from rasters, up to 1km from the station
- change day definition for precipitation to run from 5:50 to 5:50 as written in dwd cdc description. (previously it was 5:40 - 5:40, as 5:40 was the last value of the previous day)
- add ability to get all the meta information with get_meta
- save last_imp period but only for df without NAs -> else the marking of last_imp_qc... will not work, as the period will always be smaller than the last_imp period

## Version 0.0.15
- change append with pandas concat function. -> faster
- don't import complete module on installation

## Version 0.0.14
- added type test, if parameter gets checked for "all"
- specify that secrets_weatherDB file should be on PYTHONPATH environment variable
- Changed DGM5 to Copernicus DGM25, because of license advantages
- adjusted update_horizon method to be able to work with different CRS
- add kwargs to update_richter_class of StationsN
- fix get_geom with crs transforamation

## Version 0.0.13
- change the timezone allocation method of the precipitation download df
- set freq to 10 minutes of precipitation download, to be able to overwrite Values with NAs
- add remove_nas parameter to overwrite new NAs in the database. (mainly for programming changes)
- define the name of the geometry column in get_meta.

## Version 0.0.12
- add quality check for precipitation stations: delete values were the aggregated daily sum is more than double of the daily measurement
- when filling up also replace the filled_by column if it got changed
- TimestampPeriod class now also detects string inputs as date
- major error fixed: the coefficients calculation in the fillup method was the wrong way around
- for daily parameters the expand_timeseries_to_period ads now 23:50 to max_tstp_last_imp to get the period
- add vacuum cleanup method in Broker
- check precipitation df_raw for values below 0
- add stids parameter to last_imp methods of stations classes
- add an update method to stations classes, to do a complete update of the stations database data (update_raw + quality_check + fillup + richter_correct)
- only set start_tstp_last_imp values in db if update_raw is done for all the stations
  
## Version 0.0.11
- add fallback on thread if multiprocessing is not working
- cleaning up ftplib use. Always recreate a new instance and don't try to reuse the instance.
  This resolves some problems with the threading of the instances.
- clean raw updates of only recent files by the maximum timestamp of the historical data.

## Version 0.0.10
- fixed get_adj compare Timestamp with timezone 

## Version 0.0.9
- fixed future warning in stations.GroupStations().create_ts
- stations.GroupStations().create_roger_ts fixed
- removed join_how from _check_period as it was not used
- fixed StationND().get_adj, because the StationNBase.get_adj was only for 10 minute resolution
- get_adj always based on "filled" data
  
## Version 0.0.8
- fixed installation (psycopg2 problem and DB_ENG creation)
- fixed importing module when not super user

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