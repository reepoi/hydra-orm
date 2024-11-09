import dataclasses
from dataclasses import dataclass, field
import enum
from pathlib import Path

import hydra
import sqlalchemy as sa
from sqlalchemy import orm


engine = sa.create_engine('sqlite+pysqlite:///runs.sqlite')


mapper_registry = orm.registry()


MODULE_NAME = Path(__file__).stem


class CfgWithTable:
    __sa_dataclass_metadata_key__ = 'sa'

    def __init_subclass__(cls):
        return mapper_registry.mapped(dataclass(cls))


class Quality(str, enum.Enum):
    GOOD = 'good'
    BAD = 'bad'


Quality.table = sa.Table(
    Quality.__name__,
    mapper_registry.metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column(Quality.__name__, sa.String(max(len(e.value) for e in Quality)), unique=True, nullable=False),
)
@sa.event.listens_for(Quality.table, 'after_create')
def table_quality_populate(target, connection, **kwargs):
    connection.execute(
        sa.insert(Quality.table),
        [{Quality.__name__: e} for e in Quality]
    )
    # connection.commit()


# @mapper_registry.mapped
# @dataclass
class Model(CfgWithTable):
    """
    See sqlalchemy docs: https://docs.sqlalchemy.org/en/20/orm/dataclasses.html#mapping-pre-existing-dataclasses-using-declarative-style-fields
    """
    __tablename__ = __qualname__
    # __sa_dataclass_metadata_key__ = 'sa'

    id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.Integer, primary_key=True),
        omegaconf_ignore=True,
    ))
    name: str = field(default='CNN', metadata=dict(sa=sa.Column(sa.String(3))))
    name2: str = field(default='RNN', metadata=dict(sa=sa.Column(sa.String(3))))
    quality: Quality = field(default=Quality.GOOD, metadata=dict(sa=sa.Column(sa.ForeignKey(f'{Quality.__name__}.id'), nullable=False)))
    _target_: str = f'{MODULE_NAME}.{__qualname__}'


# @sa.event.listens_for(Model, 'do_orm_execute')
# @sa.event.listens_for(Model, 'before_insert')
def _map_enums(mapper, connection, target):
    for f in dataclasses.fields(target):
        if isinstance(f.type, enum.EnumMeta):
            table = f.type.table
            stmt = sa.select(table).where(getattr(table.c, f.type.__name__) == getattr(target, f.name))
            rows = connection.execute(stmt)
            _, rows = zip(*list(zip(range(2), rows)))
            assert len(rows) == 1
            setattr(target, f.name, rows[0].id)


sa.event.listen(Model, 'before_insert', _map_enums)


class Config(CfgWithTable):
    __tablename__ = __qualname__

    id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.Integer, primary_key=True),
        omegaconf_ignore=True,
    ))
    model_id: int = field(init=False, metadata=dict(
        # https://docs.sqlalchemy.org/en/20/core/metadata.html#sqlalchemy.schema.Column
        sa=sa.Column('model', sa.ForeignKey(f'{Model.__name__}.id'), unique=True, nullable=False),
        omegaconf_ignore=True,
    ))
    model: Model = field(default_factory=Model, metadata=dict(sa=orm.relationship('Model')))
    _target_: str = f'{MODULE_NAME}.{__qualname__}'
    # use __main__ if instantiation depends on globals used in __post_init__


mapper_registry.metadata.create_all(engine)

cs = hydra.core.config_store.ConfigStore.instance()
cs.store(name='config', node=Config)
