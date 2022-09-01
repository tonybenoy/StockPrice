import logging
from datetime import timedelta
from typing import List

import typer
from requests import Response
from rich import print as rprint

from extensions import session
from model import Currency

logging.getLogger().setLevel(logging.INFO)


def get_business_days(start_date, end_date):
    return sum(
        [
            (start_date + timedelta(days=i)).weekday() not in (5, 6)
            for i in range((end_date - start_date).days + 1)
        ]
    )


def get_currency_list() -> List[str]:
    # Get list of valid currencies from database and return it
    return [currency for currency, in session.query(Currency.currency_symbol).all()]


def handle_api_error(response: Response):
    # Handle API error and return error message
    if response.status_code == 400:
        log_and_exit(f"{response.json()}", 1)
    elif response.status_code == 401:
        log_and_exit("Check the API key", 1)

    elif response.status_code == 404:
        log_and_exit(
            "The resource you are looking for is not found, contact developer", 1
        )

    elif response.status_code == 429:
        log_and_exit(
            "You have exceeded your daily limit.\
             Please try again after some time or upgrade plan.",
            1,
        )

    elif response.status_code == 500:
        log_and_exit("Something went wrong, contact developer", 1)
    else:
        log_and_exit(f"{response.status_code},{response.json()['message']}", 1)


def log_and_exit(message: str, code: int = 1):
    # Log message and exit with code
    logging.debug(message)
    rprint(message)
    raise typer.Exit(code)
