# WeatherDB - module


Author: [Max Schmit](https://github.com/maxschmi)

[![Documentation Status](https://readthedocs.org/projects/weatherdb/badge/?version=latest)](https://weatherdb.readthedocs.io/latest)
[![Pipeline status](https://gitlab.uni-freiburg.de/hydrology/weatherDB/badges/master/pipeline.svg?ignore_skipped=true)](https://gitlab.uni-freiburg.de/hydrology/weatherDB/-/pipelines) 

The weather-DB module offers an API to interact with the automatically filled weather Database.

Depending on the Database user privileges you can use more or less methods of the classes.

There are 3 different sub modules with their corresponding classes.

- station:
Has a class for every type of station. E.g. PrecipitationStation (or StationP). 
One object represents one Station with one parameter. 
This object can get used to get the corresponding timeserie.
There is also a GroupStation class that groups the three parameters precipitation, temperature and evapotranspiration together for one station. If one parameter is not available this one won't get grouped.
- stations:
Is a grouping class for all the stations of one measurement parameter. E.G. PrecipitationStations (or StationsP).
Can get used to do actions on all the stations.
- broker:
This submodule has only one class Broker. This one is used to do actions on all the stations together. Mainly only used for updating the Database.

## Install

To install the package use PIP to install the Github repository:

```cmd
pip install weatherDB
```

If you also want to install the optional dependencies use:

```batch
pip install weatherDB[optionals]
```

Or to upgrade use:

```cmd
pip install weatherDB --upgrade
```

## Configuration

To start using this package you first need to setup the database. For further information please have a look at the [documentation](https://weatherdb.readthedocs.io/latest/) for a detailed [Configuration Guide](https://weatherdb.readthedocs.io/latest/setup/Configuration.html)
