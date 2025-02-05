import string
import random
from dataclasses import field

import sqlalchemy as sa


def generate_random_string(k=8, chars=string.ascii_lowercase+string.digits):
    return ''.join(random.SystemRandom().choices(chars, k=k))


def set_attr_to_func_value(table, attr_name, func, unique=True):
    def _set(mapper, connection, target):
        while True:
            setattr(target, attr_name, func())
            if not unique or connection.execute(
                sa.select(getattr(table, attr_name))
                .where(getattr(table, attr_name) == getattr(target, attr_name))
            ).first() is None:
                break
    return _set


def make_defaults_list(defaults_list):
    return field(repr=False, default_factory=lambda: defaults_list)
