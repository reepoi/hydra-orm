import typing
import dataclasses
from dataclasses import dataclass, field
import enum
from pathlib import Path
import omegaconf

import hydra
import sqlalchemy as sa
from sqlalchemy import orm


# engine = sa.create_engine('sqlite+pysqlite:///runs.sqlite')


mapper_registry = orm.registry()


MODULE_NAME = Path(__file__).stem


class CfgWithTable:
    __sa_dataclass_metadata_key__ = 'sa'

    def __init_subclass__(cls):
        return mapper_registry.mapped(dataclass(cls))


class Quality(str, enum.Enum):
    GOOD = 'good'
    BAD = 'bad'


# Quality.table = sa.Table(
#     Quality.__name__,
#     mapper_registry.metadata,
#     sa.Column('id', sa.Integer, primary_key=True),
#     sa.Column(Quality.__name__, sa.String(max(len(e.value) for e in Quality)), unique=True, nullable=False),
# )
# @sa.event.listens_for(Quality.table, 'after_create')
# def table_quality_populate(target, connection, **kwargs):
#     connection.execute(
#         sa.insert(Quality.table),
#         [{Quality.__name__: e} for e in Quality]
#     )
#     connection.commit()


class Field(CfgWithTable):
    __tablename__ = __qualname__
    __table_args__ = (sa.UniqueConstraint('flower', 'power'),)
    id: int = field(init=False, metadata=dict(
        sa=sa.Column(sa.Integer, primary_key=True),
        omegaconf_ignore=True,
    ))
    flower: int = field(default=0, metadata=dict(sa=sa.Column(sa.Integer())))
    power: int = field(default=0, metadata=dict(sa=sa.Column(sa.Integer())))
    _target_: str = field(default=f'{MODULE_NAME}.{__qualname__}', repr=False)


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
    # quality_id: int = field(init=False, repr=False, metadata=dict(
    #     sa=sa.Column(sa.ForeignKey(f'{Quality.__name__}.id'), nullable=False),
    #     omegaconf_ignore=True,
    # ))
    quality: Quality = field(default=Quality.GOOD, metadata=dict(
        sa=sa.Column(sa.Enum(Quality, values_callable=lambda x: [e.value for e in x]), nullable=False)
    ))
    fields: typing.List[Field] = field(default_factory=list, metadata=dict(sa=orm.relationship(Field.__name__, secondary=lambda: table_m2m_model_field)))
    _target_: str = field(default=f'{MODULE_NAME}.{__qualname__}', repr=False)

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
    model_id: int = field(init=False, repr=False, metadata=dict(
        # https://docs.sqlalchemy.org/en/20/core/metadata.html#sqlalchemy.schema.Column
        sa=sa.Column('model', sa.ForeignKey(f'{Model.__name__}.id'), nullable=False),
        omegaconf_ignore=True,
    ))
    model: Model = field(default_factory=Model, metadata=dict(sa=orm.relationship(Model.__name__)))
    # model: Model = field(default_factory=Model, metadata=dict(
    #     sa=sa.Column('model', sa.ForeignKey(f'{Model.__name__}.id'), nullable=False),
    # ))
    _target_: str = field(default=f'{MODULE_NAME}.{__qualname__}', repr=False)
    # use __main__ if instantiation depends on globals used in __post_init__


def create_all(engine):
    mapper_registry.metadata.create_all(engine)


def instantiate_and_insert_config(session, cfg):
    record = {}
    m2m = {}
    table = globals()[cfg['_target_'].split('.')[1]]
    table_fields = {f.name: f for f in dataclasses.fields(table)}
    for k, v in cfg.items():
        if isinstance(v, enum.Enum):
            record[k] = v
        elif isinstance(v, (dict, omegaconf.DictConfig)):
            row = instantiate_and_insert_config(session, v)
            record[k] = row
        elif isinstance(v, (list, omegaconf.ListConfig)):
            rows = [
                instantiate_and_insert_config(session, vv) for vv in v
            ]
            m2m[k] = rows
        elif k != '_target_' and table_fields[k].init:
            record[k] = v

    if len(m2m) > 0:
        table_alias_candidates = orm.aliased(
            table, sa.select(table).filter_by(**record).subquery('candidates')
        )
        subqueries = []
        for k, v in m2m.items():
            if len(v) > 0:
                table_related = v[0].__class__
                has_subset_of_relations = orm.aliased(
                    table, (
                        sa.select(table_alias_candidates.id)
                        .join(getattr(table_alias_candidates, k))
                        .where(table_related.id.in_([vv.id for vv in v]))
                        .distinct()
                    ).subquery('has_subset_of_relations')
                )
                subquery = (
                    sa.select(has_subset_of_relations.id)
                    .join(getattr(has_subset_of_relations, k))
                    .group_by(has_subset_of_relations.id)
                    .having(sa.func.count(table_related.id) == len(v))
                )
                subqueries.append(subquery)
            else:
                m2m_rel = table_fields[k].metadata['sa']
                m2m_table_col = getattr(m2m_rel.secondary.c, table.__name__)
                # m2m_related_col = getattr(m2m_rel.secondary.c, m2m_rel.argument)
                has_relation = sa.select(m2m_table_col)
                subquery = (
                    sa.select(table_alias_candidates.id)
                    .where(table_alias_candidates.id.notin_(has_relation))
                )
                subqueries.append(subquery)
        query = sa.intersect_all(*subqueries)
        candidates_query = sa.select(table_alias_candidates).where(table_alias_candidates.id.in_(query))
        candidates = session.execute(candidates_query)
        candidates = list(zip(range(2), candidates))
        assert len(candidates) <= 1
        if len(candidates) == 1:
            row = candidates[0][1][0]
            return row

    # with session.no_autoflush:
    add_row = True
    if len(m2m) == 0:
        saved_rows = session.execute(sa.select(table).filter_by(**record))
        saved_rows = list(zip(range(2), saved_rows))
        assert len(saved_rows) <= 1
        if len(saved_rows) == 1:
            row = saved_rows[0][1][0]
            add_row = False

    if add_row:
        row = table(**record)

    for k, v in m2m.items():
        setattr(row, k, v)
        add_row = True

    if add_row:
        session.add(row)
        session.commit()

    return row


def detach_config_from_session(table, row_id, session):
    # remember to set expire_on_commit in the session?
    stmt = sa.select(table).where(table.id == row_id).options(orm.joinedload('*'))
    sc = session.execute(stmt).unique().first()[0]
    return sc


def _map_enums(mapper, connection, target):
    for f in dataclasses.fields(target):
        if isinstance(f.type, enum.EnumMeta):
            table = f.type.table
            stmt = sa.select(table).where(getattr(table.c, f.type.__name__) == getattr(target, f.name))
            rows = connection.execute(stmt)
            _, rows = zip(*list(zip(range(2), rows)))
            assert len(rows) == 1
            setattr(target, f.name, rows[0].id)


cs = hydra.core.config_store.ConfigStore.instance()
cs.store(name='Config', node=Config)
