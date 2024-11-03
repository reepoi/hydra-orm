from dataclasses import dataclass, field

import hydra
import sqlalchemy as sa
from sqlalchemy import orm


engine = sa.create_engine('sqlite+pysqlite:///runs.sqlite')


mapper_registry = orm.registry()


class CfgWithTable:
    __sa_dataclass_metadata_key__ = 'sa'

    def __init_subclass__(cls):
        return mapper_registry.mapped(dataclass(cls))


# @mapper_registry.mapped
# @dataclass
class Model(CfgWithTable):
    """
    See sqlalchemy docs: https://docs.sqlalchemy.org/en/20/orm/dataclasses.html#mapping-pre-existing-dataclasses-using-declarative-style-fields
    """
    __tablename__ = 'model'
    # __sa_dataclass_metadata_key__ = 'sa'

    id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.Integer, primary_key=True),
        omegaconf_ignore=True,
    ))
    name: str = field(default='CNN', metadata=dict(sa=sa.Column(sa.String(3))))
    name2: str = field(default='RNN', metadata=dict(sa=sa.Column(sa.String(3))))
    _target_: str = 'config.Model'


class Config(CfgWithTable):
    __tablename__ = 'main'

    id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.Integer, primary_key=True),
        omegaconf_ignore=True,
    ))
    model_id: Model = field(init=False, metadata=dict(
        # https://docs.sqlalchemy.org/en/20/core/metadata.html#sqlalchemy.schema.Column
        sa=sa.Column('model', sa.ForeignKey('model.id'), unique=True, nullable=False),
        omegaconf_ignore=True,
    ))
    model: Model = field(default_factory=Model, metadata=dict(sa=orm.relationship('Model')))
    _target_: str = 'config.Config'
    # use __main__ if instantiation depends on globals used in __post_init__


mapper_registry.metadata.create_all(engine)

cs = hydra.core.config_store.ConfigStore.instance()
cs.store(name='config', node=Config)
