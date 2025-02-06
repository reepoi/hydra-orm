from dataclasses import field
import enum
from pathlib import Path
import typing

import omegaconf
import sqlalchemy as sa

import hydra_orm.utils
from hydra_orm import orm


MODULE_NAME = Path(__file__).stem


class StringEnum(str, enum.Enum):
    STRING1 = 'string1'
    STRING2 = 'string2'


class SubConfigManyToMany(orm.Table):
    value: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=1)


class SubConfigOneToMany(orm.Table):
    value: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=1)


class SubConfigOneToManySuperclass(orm.InheritableTable):
    value_superclass: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=1)


class SubConfigOneToManyInheritance1(SubConfigOneToManySuperclass):
    value: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=1)


class SubConfigOneToManyInheritance2(SubConfigOneToManySuperclass):
    value: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=1)


class SubConfigManyToManySuperclass(orm.InheritableTable):
    value_superclass: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=1)


class SubConfigManyToManyInheritance1(SubConfigManyToManySuperclass):
    value: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=1)


class Config(orm.Table):
    defaults: typing.List[typing.Any] = hydra_orm.utils.make_defaults_list([
        dict(sub_config_one_to_many_superclass=SubConfigOneToManyInheritance1.__name__),
        '_self_',
    ])
    alt_id: str = orm.make_field(orm.ColumnRequired(sa.String(8), index=True, unique=True), init=False, omegaconf_ignore=True)
    rng_seed: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=42)
    string: StringEnum = orm.make_field(orm.ColumnRequired(sa.Enum(StringEnum)), default=StringEnum.STRING1)
    sub_config_one_to_many = orm.OneToManyField(SubConfigOneToMany, required=True, default_factory=SubConfigOneToMany)
    sub_config_one_to_many_superclass = orm.OneToManyField(SubConfigOneToManySuperclass, required=True, default=omegaconf.MISSING)
    sub_config_many_to_many = orm.ManyToManyField(SubConfigManyToMany, default_factory=list)
    sub_config_many_to_many_superclass = orm.ManyToManyField(SubConfigManyToManySuperclass, default_factory=list)


sa.event.listens_for(Config, 'before_insert')(
    hydra_orm.utils.set_attr_to_func_value(Config, Config.alt_id.key, hydra_orm.utils.generate_random_string)
)


orm.store_config(Config)
orm.store_config(SubConfigOneToManySuperclass, group=Config.sub_config_one_to_many_superclass.key)
orm.store_config(SubConfigOneToManyInheritance1, group=Config.sub_config_one_to_many_superclass.key)
orm.store_config(SubConfigOneToManyInheritance2, group=Config.sub_config_one_to_many_superclass.key)
