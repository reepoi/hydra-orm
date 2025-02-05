from omegaconf import OmegaConf
import pytest
import hydra


import cs


@pytest.fixture
def engine():
    engine = cs.sa.create_engine('sqlite+pysqlite:///:memory:')
    cs.create_all(engine)
    return engine


def init_hydra_cfg(config_name, overrides):
    with hydra.initialize(version_base=None):
        return hydra.compose(config_name=config_name, overrides=overrides)


@pytest.mark.parametrize('overrides', [
    [],
    ['model.quality=BAD'],
    ['model.fields=[{power: 1}]'],
    ['model.fields=[{power: 1},{power: 2}]'],
    ['model.fields=[{power: 1},{flower: 2}]'],
])
def test_insert_then_fetch(engine, overrides):
    cfg = init_hydra_cfg('Config', overrides)
    with cs.orm.Session(engine, expire_on_commit=False) as session:
        sc = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
        session.commit()
        # detach refetches config
        sc_detached = cs.detach_config_from_session(sc.__class__, sc.id, session)

        assert sc == sc_detached


@pytest.mark.parametrize('overrides', [
    [],
    ['model.quality=BAD'],
    ['model.fields=[{power: 1}]'],
    ['model.fields=[{power: 1},{power: 2}]'],
    ['model.fields=[{power: 1},{flower: 2}]'],
])
def test_insert_deduplicate(engine, overrides):
    cfg = init_hydra_cfg('Config', overrides)
    scs = []
    with cs.orm.Session(engine, expire_on_commit=False) as session:
        for _ in range(2):
            sc = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
            session.commit()
            scs.append(sc)

        assert scs[0] == scs[1]


def test_insert_different_column_values(engine):
    with cs.orm.Session(engine, expire_on_commit=False) as session:
        cfg = init_hydra_cfg('Config', ['model.quality=GOOD'])
        sc1 = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
        session.commit()
        cfg = init_hydra_cfg('Config', ['model.quality=BAD'])
        sc2 = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))

        sc1 != sc2


@pytest.mark.parametrize('overrides', [
    [],
    ['model.fields=[{power: 1}]'],
])
def test_insert_deduplicate_m2m_strict_subset_exists(engine, overrides):
    with cs.orm.Session(engine, expire_on_commit=False) as session:
        cfg = init_hydra_cfg('Config', overrides)
        sc_ref = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
        session.commit()
        cfg = init_hydra_cfg('Config', ['model.fields=[{power: 1},{power: 2}]'])
        sc_new = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))

        assert sc_ref != sc_new


def test_insert_deduplicate_m2m_strict_superset_exists(engine):
    with cs.orm.Session(engine, expire_on_commit=False) as session:
        cfg = init_hydra_cfg('Config', ['model.fields=[{power: 1},{power: 2}]'])
        sc_ref = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
        session.commit()
        cfg = init_hydra_cfg('Config', ['model.fields=[{power: 1}]'])
        sc_new = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))

        assert sc_ref != sc_new
