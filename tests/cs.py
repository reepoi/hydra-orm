from dataclasses import field
import enum
from pathlib import Path

import omegaconf
import sqlalchemy as sa

import hydra_orm.utils
from hydra_orm import orm
from hydra_orm.orm import SQLALCHEMY_DATACLASS_METADATA_KEY as SA_KEY


MODULE_NAME = Path(__file__).stem


class StringEnum(str, enum.Enum):
    STRING1 = 'string1'
    STRING2 = 'string2'


class SubConfigManyToMany(orm.CfgWithTable):
    value: int = field(default=1, metadata={SA_KEY: orm.ColumnRequired(sa.Integer)})


class SubConfigOneToMany(orm.CfgWithTable):
    value: int = field(default=1, metadata={SA_KEY: orm.ColumnRequired(sa.Integer)})


class SubConfigOneToManySuperclass(orm.CfgWithTableInheritable):
    pass


class SubConfigOneToManyInheritance1(SubConfigOneToManySuperclass):
    pass


class Config(orm.CfgWithTable):
    alt_id: str = orm.make_field(orm.ColumnRequired(sa.String(8), index=True, unique=True), init=False, omegaconf_ignore=True)
    rng_seed: int = orm.make_field(orm.ColumnRequired(sa.Integer), default=42)
    string: StringEnum = orm.make_field(orm.ColumnRequired(sa.Enum(StringEnum)), default=StringEnum.STRING1)
    sub_config_one_to_many = orm.OneToManyField(SubConfigOneToMany, required=True, default_factory=SubConfigOneToMany)
    sub_config_one_to_many_superclass = orm.OneToManyField(SubConfigOneToManySuperclass, required=True, default_factory=SubConfigOneToManyInheritance1)
    sub_config_many_to_many = orm.ManyToManyField(SubConfigManyToMany, default_factory=list)


sa.event.listens_for(Config, 'before_insert')(
    hydra_orm.utils.set_attr_to_func_value(Config, Config.alt_id.key, hydra_orm.utils.generate_random_string)
)


orm.store_config(Config)
