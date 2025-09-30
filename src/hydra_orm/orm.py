import dataclasses
from dataclasses import dataclass, field
import enum
import functools
import typing

import hydra
import omegaconf
import sqlalchemy as sa
import sqlalchemy.orm as sa_orm


mapper_registry = sa_orm.registry()


SQLALCHEMY_DATACLASS_METADATA_KEY = 'sa'


ColumnRequired = functools.partial(sa.Column, nullable=False)


def make_field(column, omegaconf_ignore=False, metadata_extra=None, **field_kwargs):
    if 'metadata' in field_kwargs:
        raise ValueError(
            f"Found key 'metadata' keyword argument for {column=}."
            " Please pass any metadata as the keyword 'metadata_extra' instead."
        )
    metadata_extra = metadata_extra or {}
    return field(**field_kwargs, metadata={
        SQLALCHEMY_DATACLASS_METADATA_KEY: column,
        'omegaconf_ignore': omegaconf_ignore,
        **metadata_extra,
    })


@dataclass
class OneToManyField:
    config: typing.Any
    required: bool = field(default=True)
    default: typing.Optional[typing.Any] = field(default=None)
    default_factory: typing.Optional[typing.Callable] = field(default=None)
    enforce_element_type: bool = field(default=True)
    column_name: str = None


@dataclass
class ManyToManyField:
    config: typing.Any
    default: typing.Optional[typing.List[typing.Any]] = field(default=None)
    default_factory: typing.Optional[typing.List[typing.Callable]] = field(default=None)
    enforce_element_type: bool = field(default=True)
    m2m_table_name: str = None


def _db_row_hash(row):
    return hash(row.id)


class TableMetaclass(type):
    def __new__(cls, clsname, bases, attrs):
        if len(bases) == 0:
            return super().__new__(cls, clsname, bases, attrs)

        if '__annotations__' not in attrs:
            _set_attribute(attrs, '__annotations__', {})
        attrs['__sa_dataclass_metadata_key__'] = SQLALCHEMY_DATACLASS_METADATA_KEY
        _set_attribute(attrs, '__tablename__', clsname)
        _set_typed_attribute(attrs, '_target_', str, field(default=f"{attrs['__module__']}.{clsname}", repr=False))
        if 'id' not in attrs:
            _set_typed_attribute(
                attrs, 'id', int,
                field(init=False, metadata={
                    SQLALCHEMY_DATACLASS_METADATA_KEY: sa.Column(sa.Integer, primary_key=True),
                    'omegaconf_ignore': True,
                })
            )
        _set_attribute(attrs, '__hash__', _db_row_hash)

        for k, v in list(attrs.items()):
            if isinstance(v, OneToManyField):
                v_config_name = v.config if isinstance(v.config, str) else v.config.__name__
                if v_config_name == clsname:
                    raise ValueError(
                        'Columns that are foreign keys to their own table are not supported.'
                        f' This was attempted with the table {clsname}.'
                        ' Please consider adding another table for an indirect reference.'
                    )
                config_id_column = ColumnRequired if v.required else sa.Column
                attrs[f'{k}_id'] = field(init=False, repr=False, metadata={
                    SQLALCHEMY_DATACLASS_METADATA_KEY: config_id_column(v_config_name if v.column_name is None else v.column_name, sa.ForeignKey(f'{v_config_name}.id')),
                    'omegaconf_ignore': True,
                })
                attrs['__annotations__'][f'{k}_id'] = int

                config_field_kwargs = dict(metadata={SQLALCHEMY_DATACLASS_METADATA_KEY: sa_orm.relationship(v_config_name, foreign_keys=[attrs[f'{k}_id'].metadata[SQLALCHEMY_DATACLASS_METADATA_KEY]])})
                if v.default_factory is not None and v.default is not None:
                    raise ValueError(f'For the {OneToManyField.__name__} field {clsname}.{k}, specify exactly one of default={v.default} or default_factory={v.default_factory}, not both.')
                if v.default_factory is not None:
                    config_field_kwargs['default_factory'] = v.default_factory
                else:
                    config_field_kwargs['default'] = v.default
                attrs[k] = field(**config_field_kwargs)
                attrs['__annotations__'][k] = v.config if v.enforce_element_type else typing.Any
                if not v.required:
                    attrs['__annotations__'][k] = typing.Optional[attrs['__annotations__'][k]]
            elif isinstance(v, ManyToManyField):
                v_config_name = v.config if isinstance(v.config, str) else v.config.__name__
                if v_config_name == clsname:
                    raise ValueError(
                        'Columns that are foreign keys to their own table are not supported.'
                        f' This was attempted with the table {clsname}.'
                        ' Please consider adding another table for an indirect reference.'
                    )
                m2m_table = sa.Table(
                    f'{clsname}__{v_config_name}' if v.m2m_table_name is None else v.m2m_table_name,
                    mapper_registry.metadata,
                    sa.Column(clsname, sa.ForeignKey(f'{clsname}.id'), primary_key=True),
                    sa.Column(v_config_name, sa.ForeignKey(f'{v_config_name}.id'), primary_key=True),
                )
                config_field_kwargs = dict(metadata={SQLALCHEMY_DATACLASS_METADATA_KEY: sa_orm.relationship(v_config_name, secondary=m2m_table)})
                if v.default_factory is not None and v.default is not None:
                    raise ValueError(f'For the {ManyToManyField.__name__} field {clsname}.{k}, specify exactly one of default={v.default} or default_factory={v.default_factory}, not both.')
                if v.default_factory is not None:
                    config_field_kwargs['default_factory'] = v.default_factory
                else:
                    config_field_kwargs['default'] = v.default
                attrs[k] = field(**config_field_kwargs)
                attrs['__annotations__'][k] = typing.List[v.config] if v.enforce_element_type else typing.List[typing.Any]
        return mapper_registry.mapped(dataclass(super().__new__(cls, clsname, bases, attrs)))


class InheritableTableMetaclass(TableMetaclass):
    def __new__(cls, clsname, bases, attrs):
        if len(bases) == 0:
            return super().__new__(cls, clsname, bases, attrs)
        if '__mapper_args__' not in attrs:
            _set_attribute(attrs, '__mapper_args__', {})
        attrs['__mapper_args__'].update(dict(
            polymorphic_on=f'{SQLALCHEMY_DATACLASS_METADATA_KEY}_inheritance',
            polymorphic_identity=clsname,
        ))
        if '__annotations__' not in attrs:
            _set_attribute(attrs, '__annotations__', {})
        if InheritableTable in bases:
            _set_typed_attribute(
                attrs, f'{SQLALCHEMY_DATACLASS_METADATA_KEY}_inheritance', str,
                field(init=False, repr=False, metadata={
                    SQLALCHEMY_DATACLASS_METADATA_KEY: ColumnRequired(sa.String(20)),
                    'omegaconf_ignore': True,
                })
            )
        else:
            _set_typed_attribute(
                attrs, 'id', int,
                field(init=False, metadata={
                    SQLALCHEMY_DATACLASS_METADATA_KEY: sa.Column(sa.ForeignKey(f'{bases[0].__name__}.id'), primary_key=True),
                    'omegaconf_ignore': True,
                })
            )
            attrs['__mapper_args__']['inherit_condition'] = attrs['id'].metadata[SQLALCHEMY_DATACLASS_METADATA_KEY] == bases[0].id
        return super().__new__(cls, clsname, bases, attrs)


class Table(metaclass=TableMetaclass):
    pass


class InheritableTable(metaclass=InheritableTableMetaclass):
    pass


def _set_attribute(attrs, attr_name, attr_value):
    if (existing_attr_value := attrs.get(attr_name)) is not None:
        raise ValueError(
            f'Trying to set {attr_name}, but it is already defined with the value {existing_attr_value}.'
            f' Please remove any prior definitions of {attr_name}.'
        )
    attrs[attr_name] = attr_value


def _set_typed_attribute(attrs, attr_name, attr_type, attr_value):
    _set_attribute(attrs, attr_name, attr_value)
    attrs['__annotations__'][attr_name] = attr_type


def create_all(engine):
    mapper_registry.metadata.create_all(engine)


def store_config(node, group=None, name=None):
    if name is None:
        name = node.__name__
    cs = hydra.core.config_store.ConfigStore.instance()
    cs.store(group=group, name=name, node=node)


class HydraORMDatabaseHasDuplicateRowsError(Exception):
    def __init__(self, table_name, query_iterable, limit=10):
        super().__init__('')
        self.table_name = table_name
        self.limit = limit
        self.ids = [r[1] for r in zip(range(limit + 1), query_iterable)]
        self.over_limit = len(self.ids) > limit
        self.ids = self.ids[:limit]

    def __str__(self):
        res = (
            f"Database has duplicate rows in table '{self.table_name}'."
        )
        id_str = ', '.join(map(str, self.ids))
        if self.over_limit:
            res += f" The duplicate rows have these ids (showing first {self.limit} ids): {id_str}."
        else:
            res += f" The duplicate rows have these ids: {id_str}."
        return res


def instantiate_and_insert_config(session, cfg):
    if not isinstance(cfg, (omegaconf.DictConfig, dict)):
        raise ValueError(f'Tried to instantiate: {cfg=}')
    record = {}
    nonpersisted_fields = {}
    m2m = {}
    instance = hydra.utils.instantiate(cfg, _recursive_=False)
    table = instance.__class__
    cfg = dataclasses.asdict(instance)
    if 'defaults' in cfg:
        del cfg['defaults']
    table_fields = {f.name: f for f in dataclasses.fields(table)}
    for k, v in cfg.items():
        if isinstance(v, enum.Enum):
            record[k] = v
        elif isinstance(v, (dict, omegaconf.DictConfig)):
            row = instantiate_and_insert_config(session, v)
            record[k] = row
        elif isinstance(v, (list, omegaconf.ListConfig)):
            if hasattr(table, f'transform_{k}') and callable(getattr(table, f'transform_{k}')):
                transform = getattr(table, f'transform_{k}')
                rows = transform(session, v)
            else:
                rows = [
                    instantiate_and_insert_config(session, vv) for vv in v
                ]
            m2m[k] = rows
        elif k != '_target_' and table_fields[k].init:
            if SQLALCHEMY_DATACLASS_METADATA_KEY in table_fields[k].metadata:
                if hasattr(table, f'transform_{k}') and callable(getattr(table, f'transform_{k}')):
                    transform = getattr(table, f'transform_{k}')
                    v = transform(session, v)
                record[k] = v
            elif not k.endswith('_id'):
                nonpersisted_fields[k] = v

    if len(m2m) > 0:
        if table.__bases__[0] is InheritableTable:
            table_alias_candidates = sa_orm.aliased(
                table, sa.select(table).filter_by(**record, sa_inheritance=table.__mapper_args__['polymorphic_identity']).subquery('candidates')
            )
        else:
            table_alias_candidates = sa_orm.aliased(
                table, sa.select(table).filter_by(**record).subquery('candidates')
            )
        subqueries = []
        for k, v in m2m.items():
            if len(v) > 0:
                table_related = v[0].__class__
                if (
                    table_related.__bases__[0] is not Table
                    and table_related.__bases__[0] is not InheritableTable
                ):
                    table_related = table_related.__bases__[0]
                subquery = (
                    sa.select(table_alias_candidates.id)
                    .join(getattr(table_alias_candidates, k))
                    .group_by(table_alias_candidates.id)
                    .having(sa.func.sum(table_related.id.in_([vv.id for vv in v])) == len(v))
                    .having(sa.func.sum(table_related.id.notin_([vv.id for vv in v])) == 0)
                )
                subqueries.append(subquery)
            else:
                m2m_rel = table_fields[k].metadata[SQLALCHEMY_DATACLASS_METADATA_KEY]
                m2m_table_name = m2m_rel.parent.class_.__name__
                m2m_table_col = getattr(m2m_rel.secondary.c, m2m_table_name)
                # m2m_related_col = getattr(m2m_rel.secondary.c, m2m_rel.argument)
                has_relation = sa.select(m2m_table_col)
                subquery = (
                    sa.select(table_alias_candidates.id)
                    .where(table_alias_candidates.id.notin_(has_relation))
                )
                subqueries.append(subquery)
        query = sa.intersect(*subqueries)
        candidates_query = sa.select(table_alias_candidates).where(table_alias_candidates.id.in_(query))
        candidates = session.execute(candidates_query.limit(2)).all()
        if len(candidates) > 1:
            raise HydraORMDatabaseHasDuplicateRowsError(
                table.__name__,
                session.execute(sa.select(table_alias_candidates.id).where(table_alias_candidates.id.in_(query)))
            )
        if len(candidates) == 1:
            row = candidates[0][0]
            for k, v in nonpersisted_fields.items():
                setattr(row, k, v)
            return row

    # with session.no_autoflush:
    if len(m2m) == 0:
        if hasattr(table, '__mapper_args__') and 'polymorphic_identity' in table.__mapper_args__:
            saved_row_filters = {**record, 'sa_inheritance': table.__mapper_args__['polymorphic_identity']}
        else:
            saved_row_filters = record
        saved_rows = session.execute(sa.select(table).filter_by(**saved_row_filters).limit(2)).all()
        if len(saved_rows) > 1:
            raise HydraORMDatabaseHasDuplicateRowsError(
                table.__name__,
                session.execute(sa.select(table.id).filter_by(**saved_row_filters))
            )
        if len(saved_rows) == 1:
            row = saved_rows[0][0]
        else:
            row = table(**record)
            session.add(row)
            session.flush()
    else:
        for k, v in m2m.items():
            record[k] = v
        row = table(**record)
        session.add(row)
        session.flush()

    for k, v in nonpersisted_fields.items():
        setattr(row, k, v)
    # create strong references for all the rows to prevent the objects with
    # overridden non-persistent values from being garbage collected.
    # Just setting session.expire_on_commit=False works to prevserve the overrides
    # in the pytests in this project, but it does not work for this project:
    # https://github.com/Utah-Math-Data-Science/Latent-Dynamics-Data-Assimilation
    session.info[(table.__name__, row.id)] = row
    return row
