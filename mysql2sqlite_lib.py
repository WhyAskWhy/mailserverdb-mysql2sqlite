#!/usr/sbin/env python3

# https://github.com/WhyAskWhy/mysql2sqlite

"""
Library module used by the mysql2sqlite.py script. Not intended for direct use.
"""


########################################
# Modules - Standard Library
########################################

import configparser
import datetime
import logging
import logging.handlers
import os
import sqlite3
import sys


if __name__ == "__main__":
    sys.exit("This module is meant to be imported, not executed directly.")

########################################
# Modules - Library/config settings
########################################

app_name = 'mysql2sqlite'

# Create module-level logger object that inherits from "app" logger settings
log = logging.getLogger(app_name).getChild(__name__)

# TODO: How to mute messages from this library module by default?
# TODO: Set NullHandler
# log.addHandler(logging.NullHandler)

log.debug("Logging initialized for %s", __name__)


########################################
# Modules - Third party
########################################

# Upstream module, actively maintained and official recommendation
# of the MariaDB project (per their documentation).
# Available via OS packages (including apt repo) or pip.
#
# Examples:
#
# * sudo apt-get install mysql-connector-python
# * pip install mysql-connector-python --user
log.debug("Attempting to import mysql.connector module")
import mysql.connector as mysql

#######################################################
# Variables, constants
#######################################################


DATE = datetime.date.today()
TODAY = DATE.strftime('%Y-%m-%d')



#######################################################
# Classes
#######################################################

# TODO: Consider replacing class/functionality here with Python 3.2+ support for
# mapping API
#
# This support allows accessing a ConfigParser instance as a single
# dictionary with separate nested dictionaries for each section. In short, the
# two classes listed here do not appear to be needed any longer?
#
# Well, except for perhaps string to boolean conversion?
#
# https://pymotw.com/3/configparser/
# https://docs.python.org/3.5/library/configparser.html#mapping-protocol-access


class GeneralSettings(object):

    """
    Represents the user configurable settings retrieved from
    an external config file
    """

    def __init__(self, config_file_list):

        self.log = log.getChild(self.__class__.__name__)

        parser = configparser.ConfigParser()
        processed_files = parser.read(config_file_list)

        if processed_files:
            self.log.debug("CONFIG: Config files processed: %s",
                processed_files)
        else:
            self.log.error("Failure to read config files; "
                "See provided templates, modify and place in one of the "
                "supported locations: %s",
                ", ".join(config_file_list))

            raise IOError("Failure to read config files; "
                "See provided templates, modify and place in one of the "
                "supported locations: ",
                ", ".join(config_file_list))

        # Begin building object by creating dictionary member attributes
        # from config file sections/values.

        self.flags = {}
        self.mysqldb_config = {}
        self.sqlitedb_config = {}

        try:
            # Grab all values from section as tuple pairs and convert
            # to dictionaries for easy reference
            self.flags = dict(parser.items('flags'))
            self.mysqldb_config = dict(parser.items('mysqldb_config'))
            self.sqlitedb_config = dict(parser.items('sqlitedb_config'))

            # FIXME: Is there a better to handle this?
            # This is a one-off boolean flag from a separate section
            self.mysqldb_config['raise_on_warnings'] = \
                parser.getboolean('mysqldb_config', 'raise_on_warnings')

            # Convert text "boolean" flag values to true boolean values
            for key in self.flags:
                self.flags[key] = parser.getboolean('flags', key)

                self.log.debug("%s has a value of %s and a type of %s",
                    key,
                    self.flags[key],
                    type(self.flags[key]))

        except configparser.NoSectionError as error:

            self.log.exception("Unable to parse config file: %s", error)
            raise

class QuerySettings(object):

    """
    Represents the user provided SQL queries needed to pull data from MySQL
    tables and replicate to a local SQLite database
    """

    def __init__(self, config_file_list):

        self.log = log.getChild(self.__class__.__name__)

        parser = configparser.SafeConfigParser()
        processed_files = parser.read(config_file_list)

        # We've reached this point if no errors were thrown attempting
        # to read the list of config files. We now need to count the
        # number of parsed files and if zero, attempt to resolve why.

        if processed_files:
            self.log.debug("CONFIG: Config files processed: %s",
                processed_files)
        else:
            self.log.error("Failure to read config files; "
                "See provided templates, modify and place in one of the "
                "supported locations: %s",
                ", ".join(config_file_list))

            raise IOError("Failure to read config files; "
                "See provided templates, modify and place in one of the "
                "supported locations: ",
                ", ".join(config_file_list))

        # Setup an empty dictionary that we'll then populate with nested
        # dictionaries
        self.queries = {}

        # Process all sections in the config file
        for section in parser.sections():

            try:
                # Grab all values from section as tuple pairs and convert
                # to dictionaries and add to a dictionary "container" where
                # we will use the section header name as the key and the
                # nested dictionary as the value
                self.queries[section] = dict(parser.items(section))

            except configparser.NoSectionError as error:

                self.log.exception(
                    "Unable to parse '%s' section of config file: %s",
                    section, error)
                raise

class ConsoleFilterFunc(logging.Filter):

    """
    Honor boolean flags set within main script config file and only
    output specific log levels to the console.
    """

    def __init__(self, settings):
        self.settings = settings
        #print("Just proving that this function is being called")

    def filter(self, record):

        # If filter is not passed a settings object then fall back
        # to default values. This may occur if the configuration files are
        # not able to be read for one reason or another. In that situation
        # we want the error output to be as verbose as possible.
        if self.settings:
            if self.settings.flags['display_console_error_messages'] and record.levelname == 'ERROR':
                #print("Error messages enabled")
                return True
            if self.settings.flags['display_console_warning_messages'] and record.levelname == 'WARNING':
                #print("Warning messages enabled")
                return True
            if self.settings.flags['display_console_info_messages'] and record.levelname == 'INFO':
                #print("Info messages enabled")
                return True
            if self.settings.flags['display_console_debug_messages'] and record.levelname == 'DEBUG':
                #print("Debug messages enabled")
                return True
            else:
                #print("No matches")
                return False
        else:
            # Go with hard-coded default of displaying warning and error
            # messages until the settings object has been defined.
            if record.levelname == 'ERROR':
                return True
            if record.levelname == 'WARNING':
                return True

#######################################################
# Functions
#######################################################

def open_db_connection(settings, database):

    """
    Open a connection to the database and return a cursor object
    """


    ####################################################################
    # Open connections to databases
    ####################################################################

    log.debug("DB User: %s", settings.mysqldb_config["user"])
    log.debug("DB Name: %s", database)
    log.debug("DB Host Name/IP: %s", settings.mysqldb_config["host"])
    log.debug("DB Host Port: %s", settings.mysqldb_config["port"])
    log.debug("MySQL - raise_on_warnings: %s",
        settings.mysqldb_config["raise_on_warnings"])
    log.debug("MySQL - raise_on_warnings type: %s",
        type(settings.mysqldb_config["raise_on_warnings"]))

    log.info("Connecting to %s database on %s at port %s",
        database,
        settings.mysqldb_config["host"],
        settings.mysqldb_config["port"])

    try:
        mysql_connection = mysql.connect(
            user=settings.mysqldb_config['user'],
            password=settings.mysqldb_config['password'],
            host=settings.mysqldb_config['host'],
            port=settings.mysqldb_config['port'],
            database=database,
            raise_on_warnings=settings.mysqldb_config['raise_on_warnings']
        )

    except mysql.Error as error:
        log.exception("Unable to connect to database: %s", error)
        sys.exit(1)

    return mysql_connection


# Used by get_full_file_path() function, freestanding function
# in case it needs to be used elsewhere
def file_exists(full_path_to_file):
    """Verify that the file exists and is readable."""

    return bool(os.access(full_path_to_file, os.R_OK))


def file_can_be_modified(full_path_to_file):
    """Verify that the file exists and is readable."""

    return bool(os.access(full_path_to_file, os.W_OK))


# TODO: Merge the dir/file can be modified functions, change name to "item"
# or something similarly generic since the os.access check is happy with
# either type
def dir_can_be_modified(full_path_to_directory):
    """Verify that the directory exists and is readable."""

    return bool(os.access(full_path_to_directory, os.W_OK))


# TODO: Refactor to use exceptions over "look ahead logic"
def verify_sqlite_storage(general_settings, db_settings):

    """
    Precreate the database storage directory (if we have authorization to do so)
    and attempt to confirm access to the database file if it already exists.
    """

    # If target directory DOES NOT exist where we've been told to create the
    # SQLite database file ...
    if not os.path.isdir(general_settings.sqlitedb_config['base_dir']):

        log.warning("%s does not exist!",
            general_settings.sqlitedb_config['base_dir'])

        if general_settings.flags['fail_on_warnings']:
            log.error("Aborting due to missing directory!")
            sys.exit(1)

        # If the option is enabled for creating directories ...
        if general_settings.flags['create_directories']:

            log.debug("Attempting to create the %s directory structure.",
                general_settings.sqlitedb_config['base_dir'])

            # make an attempt to create the directory structure
            try:
                os.makedirs(general_settings.sqlitedb_config['base_dir'])

            # if that attempt fails, go ahead and bail
            except OSError as error:
                log.exception("Failed to create %s path to hold %s: %s",
                    general_settings.sqlitedb_config['base_dir'],
                    general_settings.sqlitedb_config['db_file'],
                    error)
                sys.exit(error)

            # We were able to create the directory structure. It is safe to proceed
            # with the rest of the script
            else:
                log.info("Created %s directory to hold SQLite db file.",
                    general_settings.sqlitedb_config['base_dir'])
                log.debug("Returning to SQLite db setup process ...")

        # directory does not exist and we do not have permission to
        # make an attempt at creating the directory structure
        else:
            # FIXME: Referencing a hard-coded filename here
            error_message = (
                "The {} directory does not exist and the option to create "
                "that directory structure is disabled. Please create it and "
                "grant your script user write access to it or confirm access "
                "and enable the option to created needed directories within "
                "the mysql2sqlite_general.ini file.".format(
                    general_settings.sqlitedb_config['base_dir']
                    )
                )
            log.error(error_message)
            sys.exit(error_message)

    else:
        # The directory exists. Does the database file?
        if os.path.exists(general_settings.sqlitedb_config['db_file']):

            # If we don't have write access to the requested file we should
            # sound the alarm and leave it to the sysadmin to fix
            if not file_can_be_modified(general_settings.sqlitedb_config['db_file']):
                log.error("%s exists, but lack of write permissions",
                        general_settings.sqlitedb_config['db_file'])
                sys.exit(1)

        # the directory exists, let us make sure that we can write to it
        # in order for the SQLite db setup to complete its work successfully
        else:

            if not dir_can_be_modified(general_settings.sqlitedb_config['base_dir']):
                log.error("%s exists, but lack of write permissions",
                        general_settings.sqlitedb_config['base_dir'])
                sys.exit(1)

            else:

                # At this point the sqlite3 db setup routines should be able
                # to create the new database and proceed with mirroring data
                pass


def import_sqlite_db_schema(db_connection, general_settings, query_settings):
    """Import SQL schema statements for fresh database file"""

    log.info('Creating SQLite database with imported schema ...')

    sqlite_db_schema = ""
    for table in query_settings.queries:
        sqlite_db_schema += "\n{};\n".format(
            query_settings.queries[table]['new'])

        # Make sure that the index key in the mysql2sqlite_queries.ini file
        # for each table has content.
        if (('index' in query_settings.queries[table])
            and (query_settings.queries[table]['index'] is not None)):
            log.debug(
                "Index query defined, appending index creation query"
                " to schema creation list.")
            sqlite_db_schema += "\n{};\n".format(
                query_settings.queries[table]['index'])
        else:
            log.debug("No index query defined, skipping index creation for %s",
                table)


    log.debug("Schema we will import: \n%s", sqlite_db_schema)

    try:
        db_connection.executescript(sqlite_db_schema)
    except sqlite3.Error as error:
        log.exception("Failed to create schema: %s", error)
        sys.exit(1)
    else:
        log.info("Created database with imported schema ...")


# TODO: Break into two separate functions
# Have this one call the function that gets number of tables
def sqlite_db_has_tables(full_path_to_db):
    """Confirms whether a SQLite database file contains any tables"""

    con = sqlite3.connect(full_path_to_db)
    cursor = con.cursor()
    cursor.execute("SELECT COUNT(name) FROM sqlite_master WHERE type='table';")

    table_count = cursor.fetchone()[0]

    log.debug("Table count: %d ", table_count)

    # Python: 0 converts to false, anything else to true
    return bool(table_count)

