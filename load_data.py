import logging

from extensions import session
from model import Example

logging.getLogger().setLevel(logging.INFO)


if __name__ == "__main__":
    example = Example(column_a="Hello", column_b="World")
    session.add(example)
    session.commit()

    # TODO: Implement the business logic
