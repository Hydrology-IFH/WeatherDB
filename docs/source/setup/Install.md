# Installation

## Install weatherDB module

To install the package use PIP to install the Github repository:

```batch
pip install weatherDB
```

If you also want to install the optional dependencies use:

```batch
pip install weatherDB[optionals]
```

Or to upgrade use:

```batch
pip install weatherDB --upgrade
```

After installing the package you need to [configure the module](<project:Configuration.md>).

## How-to install python

If you never used python before, this section is for you. 

To use this package you obviously need Python with several packages installed.

One way to install python is by installing [Anaconda](https://www.anaconda.com/products/distribution).

After the installation you should create yourself a virtual environment. This is basically a folder with all your packages installed and some definition files to set the appropriate environment variables...
To do so use (in Anaconda Terminal):

```batch
conda create --name your_environment_name python=3.8
```


Afterwards you need to activate your environment and then install the requirements:

```batch
conda activate your_environment_name
conda install shapely numpy geopandas pandas sqlalchemy
conda install -c conda-forge rasterio psycopg2 pip
pip install progressbar2
```

