# Sparkify databases

## Project Scope 
### Short description
This project has the purpose of model databases for data created by Sparkify. The idea is to make relational, normalized database, as there is not much data to cover and it will be easy to model the data and do the joins between datasets, which can be useful for many different analyses. 

### Input data

Input data should be in the '/data' directory and divided into '/data/log_data' and '/data/song_data' with corresponding .json files.

Example of the song data when changed to pandas Series using 'pd.read_json(PATH, typ='series')':

![Song Data](images/song_data.PNG)

Example of the log data when changed to pandas DataFrame using 'pd.read_json(PATH, lines=True)':

![Log Data](images/log_data.PNG)


### Output of the project and schema of the database
In detail, this project does:
- create the relational database following the star schema,
    - songplay (primary),
    - users,
    - songs,
    - artists,
    - time.
- reads the data created by the infrastructure and saved in the '/data' directory,
- preprocesses (transforms) the data,
- loads the data into datasets.

Data is prepared in the way that should be easily joined together when needed via primary keys for each dataset.

### Result

Examples of the data are presented below:

![Songplay Dataset](images/songtime.PNG)
    
![Users Dataset](images/users.PNG)

![Songs Dataset](images/songs.PNG)
    
![Artists Dataset](images/artists.PNG)

![Time Dataset](images/time.PNG)

## Prerequisites
Please check the libraries in each of the .py files to find out which libraries should be installed in your venv.

## How to use?
Firstly, the data needs to be downloaded from Udacity. 
It is not provided in this repo, if cloned.

How the data engineering should be done (from scratch)?
1. Open the terminal,
2. Run 'create_tables.py' 
3. Run 'etl.py'

If the datasets are created, just run etl.py.

If the data is already there, just run etl.py - it should avoid inserting unique data, yet it's recommended to check the code before doing so. Also, the paths can be changed in the etl.py, so that the only new or unique data is processed.

# TODO
- make the code more robust to errors,
- make the code more robust to infrastructure being down,