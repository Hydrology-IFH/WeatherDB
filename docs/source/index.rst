#####################################
Welcome to WeatherDB's documentation!
#####################################


The WeatherDB module offers an API to interact with the automatically filled WeatherDB Database.

There are 4 different main sub modules with their corresponding classes you might need to use.

- :py:data:`weatherdb.config`:
   This submodule has only one main object :py:class:`config <weatherdb.config.ConfigParser>`. This object is used to configure the module, like e.g. saving the database connection.
- :py:mod:`weatherdb.station`:
   Has a class for every type of station. E.g. :py:class:`StationP <weatherdb.station.StationP>` for precipitation.
   One object represents one Station with one parameter.
   This object can get used to get the corresponding timeserie.
   There is also a :py:class:`GroupStation <weatherdb.station.GroupStation>` class that groups the three parameters precipitation, temperature and potential evapotranspiration together for one station. If one parameter is not available for a specific station this one won't get grouped.
- :py:mod:`weatherdb.stations`:
   Is a grouping class for all the stations of one measurement parameter. E.g. :py:class:`StationsP <weatherdb.stations.StationsP>`.
   Can get used to do actions on all the stations.
- :py:mod:`weatherdb.broker`:
   This submodule has only one class :py:class:`Broker <weatherdb.broker.Broker>`. This one is used to do actions on all the stations together. Mainly only used for updating the Database.

To get started follow th installation guide and the setup guide first. After that you can use the quickstart guide to get a first impression of the module.

.. toctree::
   :maxdepth: 6
   :hidden:

   Setup <setup/setup>
   Method <Methode.md>
	API reference <api/api>
   Change-log <Changelog.md>
   License <License>