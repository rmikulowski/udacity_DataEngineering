import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

insert_template = """INSERT INTO {}
{};
"""

insert_staging_template = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
DELIMITER '{}' CSV
"""


# DROP TABLES
drop_stagingEiccodes = """DROP TABLE IF EXISTS staging_eiccodes"""
drop_eiccodes = """DROP TABLE IF EXISTS eiccodes"""
drop_stagingProdunits = """DROP TABLE IF EXISTS staging_produnits"""
drop_produnits = """DROP TABLE IF EXISTS produnits"""
drop_temps = """DROP TABLE IF EXISTS temps"""
drop_generation = """DROP TABLE IF EXISTS generation"""
drop_forecast = """DROP TABLE IF EXISTS forecast"""
drop_prices = """DROP TABLE IF EXISTS prices"""
drop_load = """DROP TABLE IF EXISTS load"""


# CREATE TABLES

create_stagingEiccodes = """CREATE TABLE IF NOT EXISTS staging_eiccodes (
EicCode varchar(256) NOT NULL,
EicDisplayName varchar(256),
EicLongName varchar(256),
EicParent varchar(256),
EicResponsibleParty varchar(256),
EicStatus varchar(256),
MarketParticipantPostalCode varchar(256), 
MarketParticipantIsoCountryCode varchar(256),
EicTypeFunctionList varchar(256)
);"""


create_eiccodes = """CREATE TABLE IF NOT EXISTS eiccodes (
EicCode varchar(256) NOT NULL,
EicDisplayName varchar(256),
EicLongName varchar(256),
MarketParticipantIsoCountryCode varchar(256)
);"""

create_stagingProdunits = """CREATE TABLE IF NOT EXISTS staging_produnits (
PU_EIC_Code varchar(256) NOT NULL,
PU_Name varchar(256),
Valid_From varchar(256),
Valid_To varchar(256),
PU_Status varchar(256),
PU_Type varchar(256),
PU_Location varchar(256),
PU_Installed_Capacity numeric(18,0),
PU_Voltage numeric(18,0),
Control_Area varchar(256),
Bidding_Zone varchar(256),
Last_Update varchar(256),
GU_Code varchar(256),
GU_Name varchar(256),
GU_Status varchar(256),
GU_Type varchar(256),
GU_Location varchar(256),
GU_Installed_Capacity numeric(18,0)
);"""

create_produnits = """CREATE TABLE IF NOT EXISTS produnits (
PUEICCode varchar(256) NOT NULL,
PUName varchar(256),
ValidFrom timestamp,
ValidTo timestamp,
PUStatus varchar(256),
PUType varchar(256),
PULocation varchar(256),
PUInstalledCapacity numeric(18,0),
ControlArea varchar(256)
);"""

create_temps = """CREATE TABLE IF NOT EXISTS temps (
dt timestamp NOT NULL,
AverageTemperature numeric(18,0),
AverageTemperatureUncertainty numeric(18,0),
City varchar(256),
Country varchar(256),
Latitude varchar(256),
Longitude varchar(256)
);"""

create_generation = """CREATE TABLE IF NOT EXISTS generation (
codingScheme varchar(256) NOT NULL,
registeredResource varchar(256),
psrType varchar(256),
MktPSRTypeName varchar(256),
MktPSRTypeText varchar(256),
time timestamp,
quantity numeric(18,3),
area varchar(256)
);"""

create_forecast = """CREATE TABLE IF NOT EXISTS forecast (
time timestamp NOT NULL,
quantity numeric(18,0),
area varchar(256)
);"""

create_prices = """CREATE TABLE IF NOT EXISTS prices (
time timestamp NOT NULL,
price numeric(18,0),
area varchar(256)
);"""

create_load = """CREATE TABLE IF NOT EXISTS load (
time timestamp NOT NULL,
quantity int4,
area varchar(256)
);"""


# INSERT TABLES

staging_names = "staging_eiccodes(EicCode, EicDisplayName, EicLongName, EicParent, EicResponsibleParty, EicStatus, MarketParticipantPostalCode, MarketParticipantIsoCountryCode, EicTypeFunctionList)"
insert_stagingEiccodes = insert_staging_template.format(staging_names, config['S3']['Y_eiccodes'], config['IAM_ROLE']['ACCESS_USER'], config['IAM_ROLE']['ACCESS_KEY'], ';')

select_stagingEiccodes = """
SELECT EicCode, EicDisplayName, EicLongName, MarketParticipantIsoCountryCode
FROM staging_eiccodes
"""
insert_eiccodes = insert_template.format('eiccodes', select_stagingEiccodes)


stagingProdunits_names = """staging_produnits
(PU_EIC_Code, PU_Name, Valid_From, Valid_To, PU_Status,
PU_Type, PU_Location, PU_Installed_Capacity, PU_Voltage, Control_Area,
Bidding_Zone, Last_Update, GU_Code, GU_Name, GU_Status, 
GU_Type, GU_Location, GU_Installed_Capacity)
"""  
insert_stagingProdunits = insert_staging_template.format(stagingProdunits_names, config['S3']['ProductionUnits'],config['IAM_ROLE']['ACCESS_USER'], config['IAM_ROLE']['ACCESS_KEY'], ';')


select_stagingProdunits = """
SELECT 
    PU_EIC_Code, PU_Name, 
    TO_DATE(Valid_From, 'DD/MM/YYYY') as valid_from,
    TO_DATE(REPLACE(Valid_To, 'Infinity', ''), 'DD/MM/YYYY') as valid_to,
    PU_Status, PU_Type, PU_Location, PU_Installed_Capacity, Control_Area
FROM staging_produnits
"""
insert_produnits = insert_template.format('produnits', select_stagingProdunits)


stagingTemps_names = "temps(dt, AverageTemperature, AverageTemperatureUncertainty, City, Country, Latitude, Longitude)"
insert_temps = insert_staging_template.format(stagingTemps_names, config['S3']['Temps'],config['IAM_ROLE']['ACCESS_USER'], config['IAM_ROLE']['ACCESS_KEY'], ',')

stagingGeneration_names = "generation(codingScheme, registeredResource, psrType, MktPSRTypeName, MktPSRTypeText, time, quantity, area)"
insert_generation = insert_staging_template.format(stagingGeneration_names, config['S3']['s3']+f'PL2017generation',config['IAM_ROLE']['ACCESS_USER'], config['IAM_ROLE']['ACCESS_KEY'], ',')

stagingForecast_names = "forecast(time, quantity, area)"
insert_forecast = insert_staging_template.format(stagingForecast_names, config['S3']['s3']+f'PL2017forecast',config['IAM_ROLE']['ACCESS_USER'], config['IAM_ROLE']['ACCESS_KEY'], ',')

stagingPrices_names = "prices(time, price, area)"
insert_prices = insert_staging_template.format(stagingPrices_names, config['S3']['s3']+f'PL2017prices',config['IAM_ROLE']['ACCESS_USER'], config['IAM_ROLE']['ACCESS_KEY'], ',')

stagingLoad_names = "load(time, quantity, area)"
insert_load = insert_staging_template.format(stagingLoad_names, config['S3']['s3']+f'PL2017load',config['IAM_ROLE']['ACCESS_USER'], config['IAM_ROLE']['ACCESS_KEY'], ',')


# QUERY LISTS

drop_table_queries = [drop_stagingEiccodes,
                      drop_stagingProdunits,
                      drop_eiccodes, 
                      drop_produnits, 
                      drop_temps, 
                      drop_generation, 
                      drop_forecast, 
                      drop_prices, 
                      drop_load]

create_table_queries = [create_stagingEiccodes,
                        create_stagingProdunits,
                        create_eiccodes, 
                        create_produnits, 
                        create_temps, 
                        create_generation, 
                        create_forecast, 
                        create_prices, 
                        create_load]

insert_table_queries = [insert_stagingEiccodes,
                        insert_stagingProdunits,
                        insert_eiccodes, 
                        insert_produnits, 
                        insert_temps, 
                        insert_generation, 
                        insert_forecast, 
                        insert_prices, 
                        insert_load]