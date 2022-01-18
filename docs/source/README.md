WeatherDB - module
==================
author: Max Schmit

The weather-DB module offers an API to interact with the automaticaly filled weather Database.

Depending on the Database user privileges you can use more or less methodes of the classes.

There are 3 different sub modules with their corresponding classes.

- station:
Has a class for every type of station. E.g. PrecipitationStation (or StationN). 
One object represents one Station with one parameter. 
This object can get used to get the corresponding timeserie.
There is also a StationGroup class that groups the three parameters precipitation, temperature and evapotranspiration together for one station. If one parameter is not available this one won't get groupped.
- stations:
Is a grouping class for all the stations of one measurement parameter. E.G. PrecipitationStations (or StationsN).
Can get used to do actions on all the stations.
- broker:
This submodule has only one class Broker. This one is used to do actions on all the stations together. Mainly only used for updating the DB.

Get started
-----------
To get started you need to enter the credentials to access the Database. If this is an account with read only acces, than only those methodes, that read data from the Database are available.
Enter those credentials in the secretSettings.py file.