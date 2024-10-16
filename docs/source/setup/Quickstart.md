# Quick-start

After installing and setting up the user configuration file with your database credentials you are ready to use the package.
This page should show you the basic usage of the package.

The package is divided in 2 main submodules:
- **weatherDB.station:**<br>
  This module has a class for every type of station. E.g. StationP (or StationP).
  One object represents one Station with one parameter.
  This object can get used to get the corresponding timeserie.
  There is also a StationGroup class that groups the three parameters precipitation, temperature and evapotranspiration together for one station.
- **weatherDB.stations:**<br>
  This module has grouping classes for all the stations of one parameter. E.G. StationsP (or StationsP) groups all the Precipitation Stations available.
  Those classes can get used to do actions on all the stations.

The basics of those modules are explained here, but every class and method has way more parameters to get exactly what you want. PLease use the API-reference for more information.

## download
### single station

If you want to download data for a single station, you have to create an object of the respective class, by gibing the station id you are interested in. This station object can then get used to download the data from the database.

If you want e.g. to download all the 10 minute, filled and Richter corrected precipitation data for the DWD station in Freiburg(station ID = 1443), then you can go like:

```
from weatherDB import StationP
stat_p = StationP(1443)
df = stat_p.get_corr()
```

If you are only interested in a small timespan, provide the period parameter with the upper and lower time limit. If e.g. you want the data for the years 2000-2010 do:
```
df = stat_p.get_corr(period=("2000-01-01", "2010-12-31"))
```

If you are not interested in the filled and Richter-corrected data, but want e.g. the raw data, add the kind parameter to your query. Like e.g.:
```
df = stat_p.get_raw()
```
Or use the more general function with the wanted kind parameter.
```
df = stat_p.get_df(kinds=["raw"])
```
There are 3-5 different kinds of timeseries available per station object depending on the class.
So there is:
- "raw" : the raw measurements as on the DWD server
- "qc"  : The quality checked data
- "filled" : The filled timeseries
- "filled_by" : The station ID of the station from which the data was taken to fill the measurements
- "corr"    : The Richter corrected timeserie.

If you want more than just one kind of timeseries, e.g. the filled timeseries, together with the id from which station the respective field got filled with use:
```
df = stat_p.get_df(kinds=["filled", "filled_by"])
```

If you only need daily values, you can hand in the agg_to parameter. This will also make your query faster, because not as much data has to get transmitted over the network.
```
df = stat_p.get_df(agg_to="day")
```

Similar to the precipitation, you can also work with the Temperature and potential Evapotranspiration data:
```
from weatherDB import StationT, StationET
stat_t = StationT(1443)
stat_et = StationET(1443)
period = ("2000-01-01", "2010-12-31")
df_t = stat_t.get_df(
    period=period,
    kinds=["raw", "filled"])
df_et = stat_t.get_df(
    period=period,
    kinds=["raw", "filled"])
```

So to download the 3 parameters N, T and ET from one station you could create the 3 single station objects and then have 3 different timeseries. But the better solution is to use the GroupStation class. This class groups all the available parameters for one location. Here is an example, how you could use it to get a Dataframe with the filled data:
```
from weatherDB import GroupStation
stat = GroupStation(1443)
df = stat.get_df(
    period=("2000-01-01", "2010-12-31"),
    kind="filled",
    agg_to="day")
```

### multiple stations
If you want to download the data for multiple stations. Like e.g. the station in Freiburg (1443) and the station on the Feldberg (1346) it is recommended to use the classes in the stations module.

To use the stations-module, you first have to create an object and then hand the station ids you are interested in when downloading it:
```
from weatherDB import StationsP
stats_n = StationsP()
df = stats_n.get_df(
    stids=[1443, 1346],
    period=("2000-01-01", "2010-12-31"),
    kind="filled")
```

## create timeseries files
You can also use the module to quickly create the csv-timeseries needed by RoGeR. Either for one station:

```
from weatherDB import GroupStation
stat = GroupStation(1443)
df = stat.create_roger_ts(
    dir="path/to/the/directory/where/to/save")
```

or for multiple stations, you can use the GroupStations. This will create a subdirectory for ever station. It is also possible to save in a zip file, by simply giving the path to a zip file. (will get created):

```
from weatherDB import GroupStations
stats = GroupStations()
df = stats.create_roger_ts(
    stids=[1443, 1346],
    dir="path/to/the/directory/where/to/save")
```
If you don't want to use the RoGeR format for the timestamp you can use the `.create_ts()` method. This method also offers you way more possibilities to define the output, like e.g. adding the share of NAs in the aggregation step or adding the filled_by column.

```
from weatherDB import GroupStations
stats = GroupStations()
df = stats.create_ts(
    stids=[1443, 1346],
    dir="path/to/the/directory/where/to/save")

# or for one station
from weatherDB import GroupStation
stat = GroupStation(1443)
df = stat.create_ts(
    dir="path/to/the/directory/where/to/save")
```

## get meta information
If you need more information about the stations you can get the meta data for a single station:

```
from weatherDB import StationP
stat = StationP(1443)
meta_dict = stat.get_meta()
```

or for multiple stations, you can use the Stations class and get a GeoDataFrame as output with all the stations information.

```
from weatherDB import StationsP
stats = StationsP(
    stids=[1443, 1346])
df = stats.get_meta()
```

Furthermore you can also get all the information for every parameter of one station by using the GroupStation class:
```
from weatherDB import GroupStation
gstat = GroupStation(1443)
df = gstat.get_meta()
```

To get an explanation about the available meta information you can use the get_meta_explanation method:
```
from weatherDB import StationP, StationsP
stat = StationP(1443)
explain_df = stat.get_meta_explanation()
# or
stats = StationsP()
explain_df = stats.get_meta_explanation()
```

If you are only interested in some information you can use the infos parameter like:
```
from weatherDB import StationP
stat = StationP(1443)
filled_period_1443 = stat.get_meta(infos=["filled_from", "filled_until"])
```
but to get the filled period you can also use the get_period method, like:
```
from weatherDB import StationP
stat = StationP(1443)
filled_period_1443 = stat.get_period_meta(kind="filled")
```

This should give you an first idea on how to use the package. Feel free to try out and read the API reference section to get more information.

