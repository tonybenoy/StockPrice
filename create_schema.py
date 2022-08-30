import logging

from extensions import engine
from model import Base

logging.getLogger().setLevel(logging.INFO)

# If you have modified models:
# - Delete or rename existing "mini.db" file
# - Run this script

if __name__ == "__main__":
    logging.info("Creating the database schema..")
    Base.metadata.create_all(engine)
