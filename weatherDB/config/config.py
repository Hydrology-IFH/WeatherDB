import configparser
from pathlib import Path
import keyring
from getpass import getpass

# set the file paths for the config files
DEFAULT_CONFIG_FILE = Path(__file__).parent/'config_default.ini'
SYS_CONFIG_FILE = Path(__file__).parent/'config_sys.ini' # set by this module
USER_CONFIG_FILE = Path(__file__).parent/'config.ini' # optional

# read the default config file
config = configparser.ConfigParser()
config.read(DEFAULT_CONFIG_FILE)

# read the user config file
config.read(SYS_CONFIG_FILE)
if USER_CONFIG_FILE.exists():
    config.read(USER_CONFIG_FILE)
    if "PASSWORD" in config["database"]:
        raise PermissionError("The password should not be in the config.ini file. Please use set.")
    config.write(SYS_CONFIG_FILE.open('w'))

# define functions
def _set_config(section, option, value):
    """The internal function to set a configuration option for the weatherDB module.

    Please use set_config instead.

    Parameters
    ----------
    section : str
        A section of the configuration file.
        See config_default.ini for available sections.
    option : str
        The option to be changed.
        See config_default.ini for available options and explanations.
    value : str, int or bool
        The new value for the option.
    """
    if value != config.get(section, option):
        config.set(section, option, value)
        with open(SYS_CONFIG_FILE, 'w') as configfile:
            config.write(configfile)

# changing configuration
def set_config(section, option, value):
    """Set a configuration option for the weatherDB module.

    Parameters
    ----------
    section : str
        A section of the configuration file.
        See config_default.ini for available sections.
    option : str
        The option to be changed.
        See config_default.ini for available options and explanations.
    value : str, int or bool
        The new value for the option.

    Raises
    ------
    PermissionError
        If you try to change the databse credentials with this function.
        Use set_db_credentials instead.
    """
    if section =="database" and (option == "PASSWORD" or option == "USER"):
        raise PermissionError("It is not possible to change the database credentials with set_config, please use set_db_credentials.")

    _set_config(section, option, value)

def set_db_credentials(user=None, password=None):
    """Set the database credentials for the weatherDB database.

    Parameters
    ----------
    user : str, optional
        The username for the database.
        If not given, the function will ask for it.
        The default is None.
    password : str, optional
        The password for the database user.
        If not given, the function will ask for it.
        The default is None.
    """
    # remove old password
    try:
        keyring.delete_password("weatherDB", config["database"]["USER"])
    except:
        pass

    # check if user is given
    if user is None:
        user = input("Please enter the username for the database: ")
    if password is None:
        password = getpass("Please enter the password for the given database user: ")

    # set new credentials
    _set_config("database", "USER", user)
    keyring.set_password("weatherDB", user, password)

def get_db_credentials():
    """Get the database credentials for the weatherDB database.

    Returns
    -------
    str, str
        The username and the password for the database.
    """
    if "user" not in config["database"]:
        print("No database credentials found. Please set them.")
        set_db_credentials()

    user = config["database"]["user"]
    password = keyring.get_password("weatherDB", user)

    return user, password
