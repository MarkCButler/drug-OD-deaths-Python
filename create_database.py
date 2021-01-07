"""Generate SQLite database from csv files."""

# Standard-library imports
from pathlib import Path
import sqlite3
import sys

# Third-party imports
import pandas as pd

DB_PATH = Path('data') / 'OD-deaths.sqlite'
SCRIPT_PATH = Path('create_tables.sql')
POPULATION_PATH = Path('data') / 'population.csv'
DEATH_COUNTS_PATH = Path('data') / 'VSRR_Provisional_Drug_Overdose_Death_Counts.csv'

# The population.csv file was created by manually modifying an .xlsx file
# downloaded from www.census.gov.  The csv file contains population estimates
# for the US and each of the 50 states.  For each of the years 2014 - 2019,
# there is an estimate of the population on July 1.
population_data = pd.read_csv(POPULATION_PATH)
population_data = population_data.melt(id_vars=population_data.columns[0:1],
                                       var_name='Year',
                                       value_name='Population')

# Extract from the csv file of death counts the columns and rows needed for
# the app.  Also filter out rows that are missing the death count, in order to
# simplify later processing.
to_load = ['State', 'Year', 'Month', 'Indicator', 'Data Value', 'State Name']
deaths_data = pd.read_csv(DEATH_COUNTS_PATH, usecols=to_load)
deaths_data.rename(columns={'Data Value': 'Value'}, inplace=True)
bool_index = (~deaths_data.State.isin(['DC', 'YC'])
              & ~deaths_data.Value.isna()
              & deaths_data.Indicator.str.contains(r'T\d|Drug Overdose Deaths')
              & ~deaths_data.Indicator.str.contains(r'incl\. methadone'))
deaths_data = deaths_data[bool_index].reset_index(drop=True)

# Add a label column to simplify data analysis.  This column will not be shown
# in the Data tab of the app, which displays only the raw data.
#
# Note that the order of the commands that define the label is significant.
# For instance, the indicator
#
# 'Opioids (T40.0-T40.4,T40.6)'
#
# is detected early in the series of cases by checking for the substring
# 'T40.0', and after this check, simple substrings such as 'T40.4' uniquely
# identify the remaining indicators.
label = deaths_data.Indicator.copy()
label[label.str.contains('T40.0')] = 'all_opioids'
label[label.str.contains('T40.1')] = 'heroin'
label[label.str.contains('T40.2')] = 'prescription_opioids'
label[label.str.contains('T40.[34]')] = 'synthetic_opioids'
label[label.str.contains('T40.5')] = 'cocaine'
label[label.str.contains('T43')] = 'other_stimulants'
label[label.str.contains('Drug Overdose')] = 'all_drug_OD'
deaths_data['Label'] = label

# Create a table that gives the full state name for each state abbreviation.
states_data = (deaths_data[['State', 'State Name']]
               .drop_duplicates(ignore_index=True)
               .rename(columns={'State Name': 'Name'}))

# The SQLite commands used to create tables define the state abbreviation in
# the 'states' table as a primary key, and state abbreviations in other tables
# are foreign keys referencing the primary key of the 'states' table.
#
# Consistent with this plan, drop the 'State Name' column from deaths_data,
# and replace the full state name in population_data by the abbreviation.
deaths_data = deaths_data.drop(columns='State Name')
to_replace = dict(zip(states_data.Name, states_data.State))
population_data.State = population_data.State.replace(to_replace)

if DB_PATH.exists():
    response = input(f'The database {DB_PATH} already exists.  '
                     'Do you wish to replace it (yes/no)? ')
    if response == 'yes':
        DB_PATH.unlink()
    else:
        print('\nExiting.\n\n')
        sys.exit()

db_con = sqlite3.connect(DB_PATH)
with db_con:
    cursor = db_con.cursor()
    cursor.executescript(SCRIPT_PATH.read_text())
    states_data.to_sql('states', con=db_con,
                       if_exists='append', index=False)
    deaths_data.to_sql('death_counts', con=db_con,
                       if_exists='append', index=False)
    population_data.to_sql('populations', con=db_con,
                           if_exists='append', index=False)

db_con.close()
