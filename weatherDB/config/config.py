import configparser
from pathlib import Path
import keyring

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
        keyring.set_password(
            "weatherDB",
            config["database"]["USER"],
            config["database"]["PASSWORD"])
        config.remove_option("database", "PASSWORD")
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

def set_db_credentials(user, password):
    """Set the database credentials for the weatherDB database.

    Parameters
    ----------
    user : str
        The username for the database.
    password : str
        The password for the database user.
    """
    # remove old password
    try:
        keyring.delete_password("weatherDB", config["database"]["USER"])
    except:
        pass

    # set new credentials
    _set_config("database", "USER", user)
    keyring.set_password("weatherDB", user, password)
