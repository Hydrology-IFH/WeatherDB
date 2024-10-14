# Configuration

The weatherDB module has several configurations you can make. Those are stored in a INI-File.

## create user configuration

To configure your weatherDB module, you need to create a user configuration file somewhere on your system. To do so:

::::{tab-set}
:sync-group: category

:::{tab-item} Python
:sync: python

```{code-block} python
import weatherDB as wdb
wdb.config.create_user_config()
```
:::

:::{tab-item} Bash
:sync: bash

```{code-block} bash
weatherDB create-user-config
```
:::

::::

This now created a INI-File with all possible configurations, but all values are commented out and you will see the default value.
After creating the file, you can edit the configuration file with any Texteditor and weatherDB will use those configurations on your next import.

## Setup database

To get started you need to setup your PostGreSQL database the weatherDB module should use.

There are two usage scenarios:
1. your working with an existing weatherDB-database someone else did setup.
   Then continue with this guide
2. you want to create your own weatherDB database instance.
   Then first follow the [hosting instructions](<Hosting.md>).

(setup-main-con)=
### setup main connection

To setup you database connection open the previously created user configuration file find the `[database:main]`{l=ini} section and uncomment the options and fill them out with your credentials:

```ini
[database:main]
; setup a database connection
; The password should not be stored in the config file, as it is a security risk.
; the module will ask for the password on first execution and store it in the keyring.
HOST = localhost
PORT = 5432
DATABASE = weatherdb
USER = weatherdb
```

To set your password you will just have to use the module for the first time and will then get prompted to enter the password. This password is then securely stored in your keyring.

:::{warning}
Don't add a setting for your password in the ini file as this would be a security risk.
:::

:::{tip}
If you use the database at the hydrology department of Freiburg, please go to the [apps.hydro.intra.uni-freiburg.de/weatherdb](https://apps.hydro.intra.uni-freiburg.de/weatherdb). There you can create yourself an account and download your login credentials from your profile page ("API Password").
:::

#### example

For example for the user *philip*, who's using the UNI Freiburg internal database *weatherdb* on host *fuhys017.public.ads.uni-freiburg.de* with port *5432* this part will look like:

```ini
[database:main]
; setup a database connection
; The password should not be stored in the config file, as it is a security risk.
; the module will ask for the password on first execution and store it in the keyring.
HOST = fuhys017.public.ads.uni-freiburg.de
PORT = 5432
DATABASE = weatherdb
USER = philip
```

### multiple connections

The weatherDB configuration also allows you to work with multiple database connections. This works similarly as with the [main database](#setup-main-connection), but you don't use the `[connection:main]`{l=ini} section of the configuration file, but add a custom connection subsection by giving it any name of your choice `[connection:my_con_name]`{l=ini}.

Then you have to tell weatherDB to use this connection:

```python
import weatherDB as wdb

wdb.config.set("database", "connection", "my_con_name")
```

Alternatively you can also set this custom database connection to be used as the default connection. To do so look for the `[connection:main]`{l=ini} section in the user configuration file and change the `connection=`{l=ini} value to your **my_con_name**:


```ini
[database]
; These are the main database settings
; The database is created with the cli command create-db-schema or the weatherDB.Broker().create_db_schema() method
; you can define multiple database connections by adding a new section like [database.connection_name], where you can define your connection name
; The connection setting defines which connection is used if not specified
connection = my_con_name
```

:::{admonition} Example
:class: tip

```ini
[database]
connection = my_con_name

[database:my_con_name]
HOST = fuhys017.public.ads.uni-freiburg.de
PORT = 5432
DATABASE = weatherdb
USER = ada
```
:::


:::{attention}
 make sure to replace **my_con_name** with your chosen name in every statement.
:::
