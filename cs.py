import typing
import dataclasses
from dataclasses import dataclass, field
import enum
from pathlib import Path
import omegaconf

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


class Field(CfgWithTable):
    __tablename__ = __qualname__
    __table_args__ = (sa.UniqueConstraint('flower', 'power'),)
    id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.Integer, primary_key=True),
        omegaconf_ignore=True
    ))
    flower: int = field(default=0, metadata=dict(sa=sa.Column(sa.Integer())))
    power: int = field(default=0, metadata=dict(sa=sa.Column(sa.Integer())))
    _target_: str = f'{MODULE_NAME}.{__qualname__}'


# @mapper_registry.mapped
# @dataclass
class Model(CfgWithTable):
    """
    See sqlalchemy docs: https://docs.sqlalchemy.org/en/20/orm/dataclasses.html#mapping-pre-existing-dataclasses-using-declarative-style-fields
    """
    __tablename__ = __qualname__
    __table_args__ = tuple()
    # __table_args__ = (sa.UniqueConstraint('name', 'name2', 'quality'),)
    # __sa_dataclass_metadata_key__ = 'sa'

    id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.Integer, primary_key=True),
        omegaconf_ignore=True,
    ))
    name: str = field(default='CNN', metadata=dict(sa=sa.Column(sa.String(3))))
    name2: str = field(default='RNN', metadata=dict(sa=sa.Column(sa.String(3))))
    quality_id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.ForeignKey(f'{Quality.__name__}.id'), nullable=False),
        omegaconf_ignore=True,
    ))
    quality: Quality = field(default=Quality.GOOD)
    fields: typing.List[Field] = field(default_factory=list, metadata=dict(sa=orm.relationship(Field.__name__, secondary=lambda: table_m2m_model_field)))
    _target_: str = f'{MODULE_NAME}.{__qualname__}'

    # def __post_init__(self):
    #     self.fields = [hydra.utils.instantiate(f) for f in self.fields]


table_m2m_model_field = sa.Table(
    f'{Model.__name__}__{Field.__name__}',
    mapper_registry.metadata,
    sa.Column(Model.__name__, sa.ForeignKey(f'{Model.__name__}.id'), primary_key=True),
    sa.Column(Field.__name__, sa.ForeignKey(f'{Field.__name__}.id'), primary_key=True),
)


class Config(CfgWithTable):
    __tablename__ = __qualname__
    __table_args__ = tuple()

    id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.Integer, primary_key=True),
        omegaconf_ignore=True,
    ))
    model_id: int = field(init=False, metadata=dict(
        # https://docs.sqlalchemy.org/en/20/core/metadata.html#sqlalchemy.schema.Column
        sa=sa.Column('model', sa.ForeignKey(f'{Model.__name__}.id'), nullable=False),
        omegaconf_ignore=True,
    ))
    model: Model = field(default_factory=Model, metadata=dict(sa=orm.relationship(Model.__name__)))
    # model: Model = field(default_factory=Model, metadata=dict(
    #     sa=sa.Column('model', sa.ForeignKey(f'{Model.__name__}.id'), nullable=False),
    # ))
    _target_: str = f'{MODULE_NAME}.{__qualname__}'
    # use __main__ if instantiation depends on globals used in __post_init__


mapper_registry.metadata.create_all(engine)


def instantiate_and_insert_config(session, cfg):
    # set unique constraint on all columns
    # do upserts with returning: https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#using-returning-with-upsert-statements
    record = {}
    m2m = {}
    for k, v in cfg.items():
        if isinstance(v, enum.Enum):
            table = v.__class__.table
            stmt = sa.select(table).where(getattr(table.c, v.__class__.__name__) == v)
            rows = session.execute(stmt)
            _, rows = zip(*list(zip(range(2), rows)))
            assert len(rows) == 1
            record[f'{k}_id'] = rows[0].id
        elif isinstance(v, (dict, omegaconf.DictConfig)):
            row = instantiate_and_insert_config(session, v)
            record[f'{k}_id'] = row.id
        elif isinstance(v, (list, omegaconf.ListConfig)):
            rows = [
                instantiate_and_insert_config(session, vv) for vv in v
            ]
            m2m[k] = rows
        # else:
        elif k != '_target_':
            record[k] = v

    table = globals()[cfg['_target_'].split('.')[1]]
    stmt = (
        sa.dialects.sqlite
        .insert(table)
        .values([record])
        .returning(table)
    )

    if len(table.__table_args__) > 0:
        stmt = (
            stmt.on_conflict_do_update(index_elements=table.__table_args__[0], set_=record)
        )
    rows = session.scalars(stmt, execution_options=dict(populate_existing=True))
    _, rows = zip(*list(zip(range(2), rows)))
    assert len(rows) == 1
    row = rows[0]

    if len(m2m) > 0:
        for k, v in m2m.items():
            setattr(row, k, v)
        session.add(row)
        session.flush()

    return row


cs = hydra.core.config_store.ConfigStore.instance()
cs.store(name='config', node=Config)
