import pytest
import hydra
import sqlalchemy as sa

from hydra_orm import orm

import cs  # import to register structured configs with hydra


@pytest.fixture
def engine():
    engine = sa.create_engine('sqlite+pysqlite:///:memory:')
    orm.create_all(engine)
    return engine


def init_hydra_cfg(config_name, overrides):
    with hydra.initialize(config_path='conf_yaml', version_base=None):
        return hydra.compose(config_name=config_name, overrides=overrides)
