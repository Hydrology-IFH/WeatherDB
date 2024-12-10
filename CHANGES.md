# Change-log

## Version 1.0.5

- fix problem with horizon calculation with holes 
- fix _check_min_date for non existing stations
- fix major error in getting multi annual raster values
- add reason for dropping in log message
- rename droped to dropped in models
- add min_date as configuration value
- check before each update_raw, if the min_date of the configuration is the same as in the database. If not expand or reduce teh timeserie
- add get_date and get_datetime as methods to config
- rename config's getlist methode to get_list to be addecuate to the rest
- set min_date to 1999-01-01
  I did some analysis and if you start earlier, you will only have one to two possible reference station in the radius of 100km from which to fill up data.
- set max_fillup_distance for p to 110 km, so at least 4 reference stations are available in the range of 1999-today
- add compression to rotaed log files
- rename "station_ma_timeseries_quotient_view" to "station_ma_timeseries_raster_quotient_view"
- fixed StationMATimeserieRasterQuotientView to not throw error on NULL values in StationMARaster
- add update_ma_timeseries method to StationsBase and Broker
 
## Version 1.0.4

- fix Brokers vacuum method and add test step for this method

## Version 1.0.3

- add tests cases for database schema upgrade and downgrade
- remove all views before database schema upgrade is done
- fix small error on downloading meta from dwd 

## Version 1.0.2

- make Broker not block itself if multiple `with self.is_any_active` are concatenated
- fix check_df_raw:
  - conversion before 2000 was wrong UTC+1 -> UTC was UTC-1 -> UTC
  - when the DWD timeserie has duplicate entries, there was an error, even though the causing columns are not used here
- derive version from git tags
- cli: set_db_version asks for confirmation
- create_db_schema gets option to define the database owner of the tables and schemas
- change amount of charachters for settings values to work with longer versions
- get meta explanations from orm models not from database comments
- create user config has now an option to define what to do on existing configuration files
- alembic database connection is now possible through alembic section in the user configuration file.
- add broker method and cli option to upgrade the database schema
- add function to query the quotients between the timeseries kinds or between the timeseries mean and the multi annual raster value

## Version 1.0.1

- add create_user_config command to CLI

## Version 1.0.0

completly reworked WeatherDB to become Version 1.0.0 to now have:

- clean user configuration, no more secrets_weatehrDB.py file
- password storage in keyring
- implement sqlalchemy models to create and maintain database structure
  - add methods to create database schema
  - add alembic for migration management
  - add stations multiannual mean values as table and a view combining all possible combinations as quotients
- add testing infrastructure
- add functions to download raster files like DEM and multi annual regionalisation rasters
- add docker image
- reworked documentation
- use new version of pET from DWD, version_2:"VPGFAO"
- fix multiple errors
- add GitHub workflows
- add Gitlab-ci pipeline
- make module work with sqlalchemy 2.0
- colorfull logs
- configure logging through weatherDB.config
- ...

## Version 0.0.45

- remove REGNIE from data-sources, ass the DWD is not providing it anymore -> wasn't really used anymore

## Version 0.0.44

- add a check in the post fillup scalling method to not exceed the maximum treshold of $50mm/10min$

## Version 0.0.43

- add check for active broker session to more methods of Broker
- reorganize the cli interface. Now there is also the option to only do the quality control, richter correction, filling or update the raw data without updating all of the database. Furthermore the database version can get set to the actual version.

## Version 0.0.42

- add a quality check for precipitations: remove single peaks above 5 mm/10min
- change the method to scale 10 minute precipitation to daily measurements:
  - Add a maximum treshold of changing the measurement to add at maximum $10mm/10min$, if the scaling would like to add more rain, then it is cutten to this maximum.
  - prevent creating single peaks higher than $5mm/10min$ through this scaling process. If the scalling would create higher single peaks, then the value is set to $5mm/10min$

## Version 0.0.41

- add a maximum treshold of $50mm/10min$ as a quality check for precipitations

## Version 0.0.40

- change roger_toolbox format to keep minute and hour values even if daily aggregation

## Version 0.0.39

- add the RoGeR Toolbox format as timeseries format. See https://github.com/Hydrology-IFH/roger for more specifications on the format
- only insert needed download time if DB_ENG is super user
- add possibility to change the column names and filenames of written out weather timeserires

## Version 0.0.38

- fix problem when updating the Richter correction to only a period.
  Previously the Richter correction did only work, when applied to the whole period (period=(None,None)).
  When a smaller period was selected, everything outside of this period got set to NULL.
  This problem existed since Version 0.0.36
- update pattern to find meta file, DWD has a second file in kl daily folder, having "mn4" in name

## Version 0.0.37

- create_ts: skip period check if already done in GroupStation or GroupStations -> previously this got checked 3 times
- add functionality to StationsBase.get_df to get multiple columns
- fix error in richter_correct from previous version

## Version 0.0.36

- throw error if Richter correction is done on empty filled timeserie
- add test for filled daily values before adjusting the 10 minute values in the fillup
- fix errors in fillup for Temperature stations
- set autocommit for _drop method
- richter_correct: only update corr when new values -> way faster
- only give aggregated value if at least 80% data is available

## Version 0.0.35

- set filled_by for T stations default to NULL not [NULL] -> works better with other methods
- change date parsing for read dwd function, to work with pandas version >2.0

## Version 0.0.34

- StationsBase.get_meta: strip whitespace in str columns
- add min/max-tresholds for T and ET
- add -9999 as possible NA value for DWD data

## Version 0.0.33

- change quality control of T- & ET-Stations -> add inversion consideration for stations above 800m altitude
  Those stations values are only sorted out in winter if their difference to the median neighbor station is negative (lower limit)
- change quality control of T and ET -> the values are now getting compared to the median of 5 neighbors, not the mean
- change fillup method: has now the possibility to take the median of multiple neighboring stations to fillup. This possibility is now used for Temperature stations, where 5 neighboring stations are considered.

## Version 0.0.32

- add elevation consideration for the selection of neighboring stations, based on LARSIM formula for the quality_check and fillup procedure of T and ET. So not only the closest stations are selected but sometimes also a station that is further away, but has les difference in height.
- get neighboring stations for T and ET quality check for full years, to always have around 5 neighboring stations
- fix problem in get_multi_annual for T Station if no ma found
- fix error because timeseries did only get created when, station T or ET is in meta_n table, even if they exist in meta_t or meta_et. So e.g a T Station exists in meta table because of own data, but is not added because no P station is there.

## Version 0.0.31

- only compare to neighboring stations if at least 2 stations have data in the quality check of T and ET
- add settings to the database and broker now updates the whole database if a new version is loaded
- stop broker execution if another broker instance is activly updating the database

## Version 0.0.30

- fix MAJOR error in Temperature quality check: The coefficient did not get converted to the database unit.
  This had as a consequence, that the neighboring values did not get regionalised correctly to the checked station. So if the neighboring station has big difference in the multi annual temperature value, too many values got kicked out.
  This error existed probably since version 0.0.15

## Version 0.0.29

-add calculation of dropped values in quality check

## Version 0.0.28

- MAJOR Error fix: The quality check for T and ET did not consider the decimal multiplier for the limits. So the table 2 from the Method documentation should have looked like this until now, in bold are the numbers that were wrong in the code:

| parameter | compare equation | lower limit | upper limit|
|:---:|:---:|:---:|:---:|
| Temperature |  $$\Delta T = T_{Stat} - \overline{T}_{neighbors}$ | $\Delta T < -\mathbf{0.5}°C$ | $\Delta T > \mathbf{0.5}°C$$ |
| pot. Evapotranspiration |  $\delta ET = \dfrac{ET_{Stat}}{\overline{ET}_{neighbors}}$ | $\begin{cases}\delta ET< \mathbf{20}\% \\ ET_{Stat}> \mathbf{0.2} \frac{mm}{d}\end{cases}$|$\begin{cases}\delta ET> 200\% \\ ET_{Stat}> \mathbf{0.3} \frac{mm}{d}\end{cases}$|

Those limits got corrected to correspond now to:
| parameter | compare equation | lower limit | upper limit|
|:---:|:---:|:---:|:---:|
| Temperature |  $\Delta T = T_{Stat} - \overline{T}_{neighbors}$ | $\Delta T < -\mathbf{5}°C$ | $\Delta T > \mathbf{5}°C$ |
| pot. Evapotranspiration |  $\delta ET = \dfrac{ET_{Stat}}{\overline{ET}_{neighbors}}$ | $\begin{cases}\delta ET< \mathbf{25}\% \\ ET_{Stat}> \mathbf{2} \frac{mm}{d}\end{cases}$|$\begin{cases}\delta ET> 200\% \\ ET_{Stat}> \mathbf{3} \frac{mm}{d}\end{cases}$|
- fixed error that came up in version 0.0.27 for richter correction. The horizon was only calculated from west to south not from north to south.
- correct update_horizon to also consider that the distance between grid cells can be diagonal to the grid, so miultiply with $\sqrt{2}$

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
