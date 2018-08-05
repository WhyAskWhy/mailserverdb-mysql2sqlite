#!/usr/bin/env python3

# https://github.com/WhyAskWhy/mysql2sqlite


"""
Query MySQL database, mirror relevant tables to a local SQLite database.
"""

# TODO:
#
# * Clearly define what conditions are "warnings" and explicitly handle them
# ** e.g., how shall the script handle a request to query a MySQL database
#    table that does not exist? This seems like an error ...
# ** what about _not_ handling a MySQL table in a particular database?
# *** perhaps that should be reported as a warning unless a flag is set to
#     disable warnings for unhandled tables?


#######################################################
# Modules - Standard Library
#######################################################

# parse command line arguments, 'sys.argv'
import argparse
import logging
import logging.handlers
import os
import os.path
import sqlite3
import sys


app_name = 'mysql2sqlite'

# TODO: Configure formatter to log function/class info
syslog_formatter = logging.Formatter('%(name)s - %(levelname)s - %(funcName)s - %(message)s')
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s')
stdout_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s')

# Grab root logger and set initial logging level
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# The SysLogHandler class, supports sending logging messages to a remote
# or local Unix syslog.
# TODO: Expose this value elsewhere; move to logging_config.json?
syslog_socket = '/dev/log'
try:
    syslog_handler = logging.handlers.SysLogHandler(address=syslog_socket)
except AttributeError:
    # We're likely running on Windows, so use the NullHandler here
    syslog_handler = logging.NullHandler
else:
    # Good thus far, finish configuring SysLogHandler
    syslog_handler.ident = app_name + ": "
    syslog_handler.setFormatter(syslog_formatter)
    syslog_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setFormatter(stdout_formatter)
# Apply lax logging level since we will use a filter to examine message levels
# and compare against allowed levels set within the main config file. This
# filter is added later once the settings config object has been constructed.
console_handler.setLevel(logging.NOTSET)

file_handler = logging.FileHandler(app_name + '.log', mode='a')
file_handler.setFormatter(file_formatter)
file_handler.setLevel(logging.DEBUG)

# Create logger object that inherits from root and will be inherited by
# all modules used by this project
# Note: The console_handler is added later after the settings config object
# has been constructed.
app_logger = logging.getLogger(app_name)
app_logger.addHandler(syslog_handler)
app_logger.addHandler(file_handler)
app_logger.setLevel(logging.DEBUG)

log = app_logger.getChild(__name__)

log.debug("Logging initialized for %s", __name__)


########################################
# Modules - Third party
########################################

# Upstream module, actively maintained and official recommendation
# of the MariaDB project (per their documentation).
#
# Available via OS packages (including apt repo) or pip.
#
# Examples:
#
# * sudo apt-get install mysql-connector-python
# * pip install mysql-connector-python --user
log.debug("Attempting to import mysql.connector module")
import mysql.connector as mysql



########################################
# Modules - Custom
########################################

import mysql2sqlite_lib as m2slib


log.debug("Finished importing standard modules and our custom library modules.")



#######################################################
# CONSTANTS - Modify INI config files instead
#######################################################

# Where this script being called from. We will try to load local copies of all
#  dependencies from this location first before falling back to default
# locations in order to support having all of the files bundled together for
# testing and portable use.
script_path = os.path.dirname(os.path.realpath(__file__))

# The name of this script used (as needed) by error/debug messages
script_name = os.path.basename(sys.argv[0])

# Read in configuration file. Attempt to read local copy first, then
# fall back to using the copy provided by SVN+Symlinks

# TODO: Replace with command-line options
default_config_file_dir = '/etc/whyaskwhy.org/mysql2sqlite/config'

general_config_file = {}
general_config_file['name'] = 'mysql2sqlite_general.ini'
general_config_file['local'] = os.path.join(script_path, general_config_file['name'])
general_config_file['global'] = os.path.join(default_config_file_dir, general_config_file['name'])

queries_config_file = {}
queries_config_file['name'] = 'mysql2sqlite_queries.ini'
queries_config_file['local'] = os.path.join(script_path, queries_config_file['name'])
queries_config_file['global'] = os.path.join(default_config_file_dir, queries_config_file['name'])

# Prefer the local copy over the "global" one by loading it last (where the
# second config file overrides or "shadows" settings from the first)
general_config_file_candidates = [general_config_file['global'], general_config_file['local']]

queries_config_file_candidates = [queries_config_file['global'], queries_config_file['local']]

# Generate configuration setting options
log.debug(
    "Passing in these general config file locations for evalution: %s",
    general_config_file_candidates)

log.debug(
    "Passing in these query config file locations for evalution: %s",
    queries_config_file_candidates)

# Generate configuration setting options
log.info('Parsing config files')
general_settings = m2slib.GeneralSettings(general_config_file_candidates)
query_settings = m2slib.QuerySettings(queries_config_file_candidates)

# Now that the settings object has been properly created, lets use it to
# finish configuring console logging for the main application logger.
console_handler.addFilter(m2slib.ConsoleFilterFunc(settings=general_settings))
app_logger.addHandler(console_handler)

####################################################################
# Troubleshooting config file flag boolean conversion
####################################################################

# Troubleshooting config file flag boolean conversion
for key, value in list(general_settings.flags.items()):
    log.debug("key: '%s' value: '%s' type of value: '%s'",
        key,
        value,
        type(value))


####################################################################
# Open connections to databases
####################################################################

log.info("Opening connection to MySQL database")
mysql_connection = m2slib.open_db_connection(general_settings,
    general_settings.mysqldb_config['database'])


# If SQLite database doesn't already exist we will need to import the schema
# when we create the database. Set a flag to indicate that need.
SQLITE_DB_IS_NEW = not os.path.exists(
    general_settings.sqlitedb_config['db_file'])

if SQLITE_DB_IS_NEW:

    # Make sure that one of these is true:
    # * We have write access to an existing SQLite database file
    # * We have write access to the folder where we will create the SQLite database
    #
    # TODO: This function is too large, does too much
    try:
        m2slib.verify_sqlite_storage(general_settings, query_settings)
    except IOError as error:
        log.exception(error)
        sys.exit(1)

# Open a connection to the database.
try:
    sqlite_connection = sqlite3.connect(
        general_settings.sqlitedb_config['db_file'])
except sqlite3.Error as error:
    log.exception("Failed to connect to the %s database: %s",
        general_settings.sqlitedb_config['db_file'],
        error)
    sys.exit(1)
else:
    log.info("Connected to SQLite database ...")


# Make sure that there are tables in the database
if not m2slib.sqlite_db_has_tables(general_settings.sqlitedb_config['db_file']):
    log.debug("SQLite db file has no tables")
    SQLITE_DB_MISSING_TABLES = True
else:
    log.debug("SQLite db file has one more more tables")
    SQLITE_DB_MISSING_TABLES = False


# If the database is newly created or missing tables import the schema
if SQLITE_DB_IS_NEW or SQLITE_DB_MISSING_TABLES:
    log.info("%s database file is new or is missing tables; importing schema." ,
        general_settings.sqlitedb_config['db_file'])

    m2slib.import_sqlite_db_schema(sqlite_connection,
        general_settings, query_settings)
else:
    log.info("%s database file exists and has tables; skipping schema import",
        general_settings.sqlitedb_config['db_file'])

# Verify that autocommit is turned off
if sqlite_connection.isolation_level is None:

    if general_settings.flags['fail_on_warnings']:

        log.warning("autocommit mode is enabled. "
            "This results in poor performance when many updates are required.")
else:
    log.info("autocommit mode is disabled. "
               "This should help performance for large batches of updates")


####################################################################
# Create cursor objects so that we can interact with the databases
####################################################################

# Cursor for the SQLite copy of the database
sqlite_cursor = sqlite_connection.cursor()

# Cursor for the MySQL copy of the database
mysql_cursor = mysql_connection.cursor()



####################################################################
# Copy data from primary MySQL database to local SQLite database
####################################################################

log.info("Continuing with db prep work ...")

# To retrieve data after executing a SELECT statement, you can either treat the
# cursor as an iterator, call the cursor's fetchone() method to retrieve a
# single matching row, or call fetchall() to get a list of the matching rows.
#
# Works for all tables except for the 'virtual_users' table as I've opted to
# not include passwords in the local SQLite db files (at least for now)

################################################################################
# Official Python SQLite docs
#
# When a database is accessed by multiple connections, and one of the
# processes modifies the database, the SQLite database is locked until that
# transaction is committed. The timeout parameter specifies how long the
# connection should wait for the lock to go away until raising an exception.
# The default for the timeout parameter is 5.0 (five seconds).
################################################################################

if m2slib.sqlite_db_has_tables(general_settings.sqlitedb_config['db_file']):

    DROP_TABLES = True
    log.debug("Tables were found. Setting flag to drop tables.")

else:

    # Set flag so we won't try to clear table contents from non-existent tables
    DROP_TABLES = False
    log.debug("Tables were not found. Setting flag to skip dropping tables.")

log.debug("MySQL tables to replicate: %s",
    ", ".join(query_settings.queries.keys()))

for table in query_settings.queries:

    # Dynamically create the select query used to pull data from MySQL table
    mysql_cursor.execute(query_settings.queries[table]['read'])

    # FIXME: A future version will first check to make sure that there are
    # new entries which require regenerating/updating the tables
    #
    if DROP_TABLES:

        log.info("Recreating %s SQLite table", table)

        try:
            log.debug("Dropping %s ...", table)
            sqlite_cursor.execute('DROP TABLE IF EXISTS {}'.format(table))

        except sqlite3.Error as error:
            log.exception(error)
            sys.exit(1)

        try:
            log.debug("Creating %s ...", table)
            sqlite_cursor.execute(query_settings.queries[table]['new'])

        except sqlite3.Error as error:
            log.exception(error)
            sys.exit(1)

        # Make sure that the index key in the mysql2sqlite_queries.ini file
        # for each table has content.
        if (('index' in query_settings.queries[table])
            and (query_settings.queries[table]['index'] is not None)):
            try:
                log.debug("Creating index for %s ...", table)
                sqlite_cursor.execute(query_settings.queries[table]['index'])

            except sqlite3.Error as error:
                log.exception(error)
                sys.exit(1)
        else:
            log.debug(
                "No index query defined,"
                " skipping index recreation for %s",
                query_settings.queries[table])


    log.info("Pulling data from %s MySQL table ...", table)
    mysql_data = mysql_cursor.fetchall()

    log.info("Updating %s SQLite table ...", table)
    for row in mysql_data:

        log.debug("Writing entries to %s SQLite table", table)

        # Add MySQL table entries to SQLite db tables
        try:
            sqlite_cursor.execute(query_settings.queries[table]['write'], (row))

        except sqlite3.Error as error:
            log.exception("Failed to write entries to %s SQLite table: %s",
                table, row)
            log.exception(error)
            sys.exit(1)

    # If we made it this far then all data has been inserted into the SQLite db table
    log.info("Updates to %s table are complete. "
        "The last inserted id was: %s\n", table, sqlite_cursor.lastrowid)


####################################################################
# Cleanup
####################################################################

# Python SQLite docs, Python Standard Library by Example
#
# This method commits the current transaction. If you don't call this method,
# anything you did since the last call to commit() is not visible from other
# database connections. This requirement gives an application an opportunity
# to make several related changes together, so they are stored atomically
# instead of incrementally, and avoids a situation where partial updates
# are seen by different clients connecting to the database simultaneously.
log.info("Committing transactions ...")
sqlite_connection.commit()

# FIXME: The values don't appear to be accurate
# NOTE: tested with Python 2.7.12, MySQL Connector against  MariaDB 10.0.29
#
# log.debug("MySQL db rows processed: %d", mysql_cursor.rowcount)
# log.debug("SQLite db rows processed: %d", sqlite_cursor.rowcount)

# Close database connections
log.info("Closing MySQL database connection ...")
mysql_connection.close()

log.info("Closing SQLite database connection ...")
sqlite_connection.close()

log.info("Transactions committed, database connections closed")
