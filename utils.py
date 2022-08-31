import logging
from datetime import timedelta
from typing import List

from requests import Response

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
        logging.info(f"{response.json()}")
    elif response.status_code == 401:
        logging.info("Check the API key")
    elif response.status_code == 404:
        logging.info("The resource you are looking for is not found, contact developer")
    elif response.status_code == 429:
        logging.info(
            "You have exceeded your daily limit.\
             Please try again after some time or upgrade plan."
        )
    elif response.status_code == 500:
        logging.info("Something went wrong, contact developer")
    else:
        logging.info(f"{response.status_code},{response.json()['message']}")
