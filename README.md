# Extract Transcribe Load

***

## Description


## Cloning the repository
```
$ git clone https://github.com/1byte-yoda/transcribe-etl.git
```

## Installing Prerequisites
If you want to run the project in docker, please follow the instruction and install docker from this [link](https://docs.docker.com/get-docker/)

## Usage

### Using Docker

```
$ docker build -t transcribe_etl .
$ docker run transcribe_etl
```

### Using your Local Computer Running Python3
Just take note that this project was tested and developed using Python 3.9.1, please check if you have the right version installed.
```
$ python --version
Python 3.9.1
```

```
$ pip install -r requirements.txt
$ python3 main.py
```

## Data Flow

## Testing and Development
You will need to install additional package if you want to run test suites and/or develop
for this application.

### Installing Requirements For Development
```
$ pip install -r requirements-dev.txt
```

### Useful Make Commands

#### Running Test Suites
```
$ make test 
```

#### Running Code Autoformat
Note: I am using `max-line-length` of 180
```
$ make black
```

#### Running Flake8 Lint
```
$ make lint
```

#### Generating HTML Test Coverage
The generated report can be found inside the `htmlcov` folder
```
$ make cov_report html
```

