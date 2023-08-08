# UFO Data Pipeline
An ETL pipeline built using python/pandas.

Pulls data from multiple sources into a target repository for analysis. Includes an example aggregating UFO sighting data from 2020-2023.

## Installation
After cloning the repository, download the project dependencies by running the following command in the root of the project:
```
pip install -r requirements.txt
```

## Usage
To run the project from the command line:
```
py main.py [-h] [-f {csv,xlsx,json,sqlite}] outfile
```

## License

[MIT](https://choosealicense.com/licenses/mit/)