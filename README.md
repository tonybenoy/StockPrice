# Stock Price Tracker

## Setup
The project uses poetry for dependency management. The dependencies are available in requirements.txt too.

Install the requirements with poetry or pip
```
poetry install --no-root
```

```
pip install -r requirements.txt
```

## Usage
Add your API key for market and exchange in constants.py.
Run the app as following with the start date, end date, currency and symbol as below
```
python load_data.py --symbol GOOGL --currency eur --start-date 2021/11/15 --last-date 2021-11-21
```

## Running tests
```
pytest
```
