import pytest
from omegaconf import OmegaConf
import sqlalchemy as sa
import sqlalchemy.orm as sa_orm

from fixtures import init_hydra_cfg, engine
import cs

from hydra_orm import orm


@pytest.mark.parametrize('overrides', [
    [],
    ['string=STRING2'],
    ['sub_config_many_to_many=[{value:1}]'],
    ['sub_config_many_to_many=[{value:1},{value:2}]'],
    ['sub_config_many_to_many_custom_m2m_table_name=[{value:1}]'],
    ['sub_config_many_to_many_custom_m2m_table_name=[{value:1},{value:2}]'],
    ['sub_config_one_to_many_superclass=SubConfigOneToManySuperclass'],
    ['sub_config_one_to_many_superclass=SubConfigOneToManyInheritance1'],
    ['sub_config_one_to_many_superclass=SubConfigOneToManyInheritance2'],
    ['sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManySuperclass}]'],
    ['sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1}]'],
    ['sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance2}]'],
    ['sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1},{_target_:cs.SubConfigManyToManyInheritance2}]'],
])
def test_insert_then_fetch_all_defaults(engine, overrides):
    cfg = init_hydra_cfg('Config', overrides)
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        cfg = orm.instantiate_and_insert_config(session, cfg)
        session.commit()
        cfg_fetched = session.execute(sa.select(cs.Config).where(cs.Config.id == cfg.id)).first()

        assert cfg_fetched is not None
        assert cfg == cfg_fetched[0]


@pytest.mark.parametrize('overrides', [
    ['sub_config_one_to_many_superclass=SubConfigOneToManySuperclass'],
    ['sub_config_one_to_many_superclass=SubConfigOneToManyInheritance1'],
])
def test_inheritance_polymorphism_column_hidden(engine, overrides):
    cfg = init_hydra_cfg('Config', overrides)
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        cfg = orm.instantiate_and_insert_config(session, cfg)
        assert f'{orm.SQLALCHEMY_DATACLASS_METADATA_KEY}_inheritance=' not in repr(cfg)


@pytest.mark.parametrize('overrides', [
    ['sub_config_one_to_many_superclass=SubConfigOneToManySuperclass'],
    ['sub_config_one_to_many_superclass=SubConfigOneToManyInheritance1'],
    ['sub_config_one_to_many_superclass=SubConfigOneToManyInheritance2'],
])
def test_deduplicate_one_to_many(engine, overrides):
    cfg_dict = init_hydra_cfg('Config', overrides)
    cfgs = []
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        for _ in range(2):
            cfg = orm.instantiate_and_insert_config(session, cfg_dict)
            session.commit()
            cfgs.append(cfg)

        cfg_a, cfg_b = cfgs
        assert cfg_a.sub_config_one_to_many == cfg_b.sub_config_one_to_many
        assert cfg_a.sub_config_one_to_many_superclass == cfg_b.sub_config_one_to_many_superclass


def test_superclass_defaults_are_set_when_only_given_subclass_overrides(engine):
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        # commit all defaults
        cfg_dict = init_hydra_cfg(
            'Config',
            ['sub_config_one_to_many_superclass_element_type_not_enforced={_target_:cs.SubConfigOneToManyInheritance1}'],
        )
        cfg_default_value_superclass = orm.instantiate_and_insert_config(session, cfg_dict)
        session.commit()

        # override superclass value
        cfg_dict = init_hydra_cfg(
            'Config',
            [f'sub_config_one_to_many_superclass_element_type_not_enforced={{_target_:cs.SubConfigOneToManyInheritance1,value_superclass:{cfg_default_value_superclass.sub_config_one_to_many_superclass_element_type_not_enforced.value_superclass+1}}}'],
        )
        orm.instantiate_and_insert_config(session, cfg_dict)
        session.commit()

        # override only subclass value
        cfg_dict = init_hydra_cfg(
            'Config',
            [f'sub_config_one_to_many_superclass_element_type_not_enforced={{_target_:cs.SubConfigOneToManyInheritance1,value:{cfg_default_value_superclass.sub_config_one_to_many_superclass_element_type_not_enforced.value}}}'],
        )
        cfg = orm.instantiate_and_insert_config(session, cfg_dict)
        session.commit()

        assert cfg.sub_config_one_to_many_superclass_element_type_not_enforced.value_superclass == cfg_default_value_superclass.sub_config_one_to_many_superclass_element_type_not_enforced.value_superclass


@pytest.mark.parametrize('overrides', [
    ['sub_config_many_to_many=[{value:1}]'],
    ['sub_config_many_to_many=[{value:1},{value:2}]'],
    ['sub_config_many_to_many_custom_m2m_table_name=[{value:1}]'],
    ['sub_config_many_to_many_custom_m2m_table_name=[{value:1},{value:2}]'],
    ['sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManySuperclass}]'],
    ['sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1}]'],
    ['sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance2}]'],
    ['sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1},{_target_:cs.SubConfigManyToManyInheritance2}]'],
    ['sub_config_many_to_many=[{value:1},{value:2}]', 'sub_config_many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1},{_target_:cs.SubConfigManyToManyInheritance2}]'],
])
def test_deduplicate_many_to_many(engine, overrides):
    cfg_dict = init_hydra_cfg('Config', overrides)
    cfgs = []
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        for _ in range(2):
            cfg = orm.instantiate_and_insert_config(session, cfg_dict)
            session.commit()
            cfgs.append(cfg)

        cfg_a, cfg_b = cfgs
        assert cfg_a.sub_config_many_to_many == cfg_b.sub_config_many_to_many
        assert cfg_a.sub_config_many_to_many_superclass == cfg_b.sub_config_many_to_many_superclass


@pytest.mark.parametrize('overrides', [
    ['sub_config_one_to_many.many_to_many=[{value:1}]'],
    ['sub_config_one_to_many.many_to_many=[{value:1},{value:2}]'],
    ['sub_config_one_to_many.many_to_many_superclass=[{_target_:cs.SubConfigManyToManySuperclass}]'],
    ['sub_config_one_to_many.many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1}]'],
    ['sub_config_one_to_many.many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance2}]'],
    ['sub_config_one_to_many.many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1},{_target_:cs.SubConfigManyToManyInheritance2}]'],
])
def test_deduplicate_nested_many_to_many(engine, overrides):
    cfg_dict = init_hydra_cfg('Config', overrides)
    cfgs = []
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        for _ in range(2):
            cfg = orm.instantiate_and_insert_config(session, cfg_dict)
            session.commit()
            cfgs.append(cfg)

        cfg_a, cfg_b = cfgs
        assert cfg_a.sub_config_one_to_many == cfg_b.sub_config_one_to_many
        assert cfg_a.sub_config_one_to_many_superclass == cfg_b.sub_config_one_to_many_superclass


@pytest.mark.parametrize('overrides', [
    ['sub_config_one_to_many.many_to_many=[]'],
    ['sub_config_one_to_many.many_to_many=[{value:1}]'],
])
def test_different_when_many_to_many_strict_subset_exists(engine, overrides):
    cfg_subset = init_hydra_cfg('Config', overrides)
    cfg = init_hydra_cfg('Config', ['sub_config_one_to_many.many_to_many=[{value:1},{value:2}]'])
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        cfg_subset = orm.instantiate_and_insert_config(session, cfg_subset)
        cfg = orm.instantiate_and_insert_config(session, cfg)
        session.commit()

        assert cfg_subset.sub_config_one_to_many != cfg.sub_config_one_to_many


@pytest.mark.parametrize('overrides', [
    ['sub_config_one_to_many.many_to_many_superclass=[]'],
    ['sub_config_one_to_many.many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1}]'],
])
def test_different_when_many_to_many_superclass_strict_subset_exists(engine, overrides):
    cfg_subset = init_hydra_cfg('Config', overrides)
    cfg = init_hydra_cfg('Config', ['sub_config_one_to_many.many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1},{_target_:cs.SubConfigManyToManyInheritance2}]'])
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        cfg_subset = orm.instantiate_and_insert_config(session, cfg_subset)
        cfg = orm.instantiate_and_insert_config(session, cfg)
        session.commit()

        assert cfg_subset.sub_config_one_to_many != cfg.sub_config_one_to_many


@pytest.mark.parametrize('overrides', [
    ['sub_config_one_to_many.many_to_many=[]'],
    ['sub_config_one_to_many.many_to_many=[{value:1}]'],
])
def test_different_when_many_to_many_strict_superset_exists(engine, overrides):
    cfg_superset = init_hydra_cfg('Config', ['sub_config_one_to_many.many_to_many=[{value:1},{value:2}]'])
    cfg = init_hydra_cfg('Config', overrides)
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        cfg_superset = orm.instantiate_and_insert_config(session, cfg_superset)
        cfg = orm.instantiate_and_insert_config(session, cfg)
        session.commit()

        assert cfg.sub_config_one_to_many != cfg_superset.sub_config_one_to_many


@pytest.mark.parametrize('overrides', [
    ['sub_config_one_to_many.many_to_many_superclass=[]'],
    ['sub_config_one_to_many.many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1}]'],
])
def test_different_when_many_to_many_superclass_strict_superset_exists(engine, overrides):
    cfg_superset = init_hydra_cfg('Config', ['sub_config_one_to_many.many_to_many_superclass=[{_target_:cs.SubConfigManyToManyInheritance1},{_target_:cs.SubConfigManyToManyInheritance2}]'])
    cfg = init_hydra_cfg('Config', overrides)
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        cfg_superset = orm.instantiate_and_insert_config(session, cfg_superset)
        cfg = orm.instantiate_and_insert_config(session, cfg)
        session.commit()

        assert cfg.sub_config_one_to_many != cfg_superset.sub_config_one_to_many


def test_different_sub_config_values(engine):
    cfgs = []
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        for i in range(2):
            cfg_dict = init_hydra_cfg('Config', [f'sub_config_one_to_many.value={i}', f'sub_config_one_to_many_superclass.value={i}'])
            cfg = orm.instantiate_and_insert_config(session, cfg_dict)
            session.commit()
            cfgs.append(cfg)

    cfg_a, cfg_b = cfgs
    assert cfg_a.sub_config_one_to_many != cfg_b.sub_config_one_to_many
    assert cfg_a.sub_config_one_to_many_superclass != cfg_b.sub_config_one_to_many_superclass


def test_dynamic_fetching_of_one_referenced_row(engine):
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        cfg = init_hydra_cfg('Config', [])
        config_alt_id = orm.instantiate_and_insert_config(session, cfg).alt_id
        cfg = init_hydra_cfg('Config', [f'one_reference.config={config_alt_id}'])
        cfg = orm.instantiate_and_insert_config(session, cfg)


def test_dynamic_fetching_of_list_of_referenced_row(engine):
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        config_alt_ids = []
        for i in range(2):
            cfg = init_hydra_cfg('Config', [f'sub_config_one_to_many.value={i}'])
            config_alt_ids.append(orm.instantiate_and_insert_config(session, cfg).alt_id)
        cfg = init_hydra_cfg('Config', [f"list_of_references=[{','.join(f'{{config:{c}}}' for c in config_alt_ids)}]"])
        cfg = orm.instantiate_and_insert_config(session, cfg)

        assert {ref.config.alt_id for ref in cfg.list_of_references} == set(config_alt_ids)
