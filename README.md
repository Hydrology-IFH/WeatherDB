WeatherDB - module
==================


author: [Max Schmit](https://github.com/maxschmi)

[![Documentation Status](https://readthedocs.org/projects/weatherdb/badge/?version=latest)](https://weatherdb.readthedocs.io/en/latest/?badge=latest)

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

Install
-------

To install the package use PIP to install the Github repository:

```cmd
pip install git+https://gitlab.uni-freiburg.de/hydrology/weatherDB.git
```

Or to upgrade use:

```cmd
pip install git+https://gitlab.uni-freiburg.de/hydrology/weatherDB.git --upgrade
```

Get started
-----------

To get started you need to enter the credentials to access the Database. If this is an account with read only access, than only those method's, that read data from the Database are available.
Enter those credentials in the secretSettings_weatherDB.py file. An example secretSettings_weatherDB.py file is in the source directory (see secretSettings_weatherDB_example.py)

If you use the database at the hydrology department of Freiburg, please go to the [apps.hydro.intra.uni-freiburg.de/weatherdb](https://apps.hydro.intra.uni-freiburg.de/weatherdb). There you can create yourself an account and download your login credentials from your profile page ("API Password").

First you need to setup your database and tell the weatherDB module your database credentials. 
There are two scenarios:
1. your working with an existing weatherDB-database
2. you want to create your own weatherDB database instance

If you'r case 2, then first follow the hosting instructions, before continuing this guide.

To configure your weatherDB module, you need to create a user configuration file somewhere on your system. To do so:

  ```python [g1:python]
  import weatherDB as wdb
  wdb.config.create_user_config()
  ```
///
=== "Tab 2"
  ```cmd [g1:cmd]
  weatherDB create-user-config
  ```
The secretSettings_weatherDB.py file needs to be placed either:

- in a parent folder of the package (e.g. in the main folder of your virtual environment folder)
- some other directory that is in the PYTHONPATH environment variable. (You can also create a new directory and add it to the PATH environment)
- in the package source folder (e.g. ../path_to_venv/Lib/site-packages/weatherDB) !This might not be the best method, because an upgrade of the package could delete the file again!

How-to install python
---------------------

To use this package you obviously need Python with several packages installed.

One way to install python is by installing [Anaconda](https://www.anaconda.com/products/distribution).

After the installation you should create yourself a virtual environment. This is basically a folder with all your packages installed and some definition files to set the appropriate environment variables...
To do so use (in Anaconda Terminal): 

```cmd
conda create --name your_environment_name python=3.8
```

Afterwards you need to activate your environment and then install the requirements:

```cmd
conda activate your_environment_name
conda install shapely numpy geopandas pandas sqlalchemy
conda install -c conda-forge rasterio psycopg2 pip
pip install progressbar2
```
