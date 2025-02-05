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
])
def test_insert_then_fetch_all_defaults(engine, overrides):
    cfg = init_hydra_cfg('Config', overrides)
    with sa_orm.Session(engine, expire_on_commit=False) as session:
        cfg = orm.instantiate_and_insert_config(session, cfg)
        session.commit()
        cfg_fetched = session.execute(sa.select(cs.Config).where(cs.Config.id == cfg.id)).first()
        assert cfg_fetched is not None
        assert cfg == cfg_fetched[0]


# @pytest.mark.parametrize('overrides', [
#     [],
#     ['model.quality=BAD'],
#     ['model.fields=[{power: 1}]'],
#     ['model.fields=[{power: 1},{power: 2}]'],
#     ['model.fields=[{power: 1},{flower: 2}]'],
# ])
# def test_insert_then_fetch(engine, overrides):
#     cfg = init_hydra_cfg('Config', overrides)
#     with sa_orm.Session(engine, expire_on_commit=False) as session:
#         sc = orm.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
#         session.commit()
#         # detach refetches config
#         sc_detached = cs.detach_config_from_session(sc.__class__, sc.id, session)
#
#         assert sc == sc_detached
#
#
# @pytest.mark.parametrize('overrides', [
#     [],
#     ['model.quality=BAD'],
#     ['model.fields=[{power: 1}]'],
#     ['model.fields=[{power: 1},{power: 2}]'],
#     ['model.fields=[{power: 1},{flower: 2}]'],
# ])
# def test_insert_deduplicate(engine, overrides):
#     cfg = init_hydra_cfg('Config', overrides)
#     scs = []
#     with sa_orm.Session(engine, expire_on_commit=False) as session:
#         for _ in range(2):
#             sc = orm.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
#             session.commit()
#             scs.append(sc)
#
#         assert scs[0] == scs[1]
#
#
# def test_insert_different_column_values(engine):
#     with sa_orm.Session(engine, expire_on_commit=False) as session:
#         cfg = init_hydra_cfg('Config', ['model.quality=GOOD'])
#         sc1 = orm.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
#         session.commit()
#         cfg = init_hydra_cfg('Config', ['model.quality=BAD'])
#         sc2 = orm.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
#
#         sc1 != sc2
#
#
# @pytest.mark.parametrize('overrides', [
#     [],
#     ['model.fields=[{power: 1}]'],
# ])
# def test_insert_deduplicate_m2m_strict_subset_exists(engine, overrides):
#     with sa_orm.Session(engine, expire_on_commit=False) as session:
#         cfg = init_hydra_cfg('Config', overrides)
#         sc_ref = orm.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
#         session.commit()
#         cfg = init_hydra_cfg('Config', ['model.fields=[{power: 1},{power: 2}]'])
#         sc_new = orm.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
#
#         assert sc_ref != sc_new
#
#
# def test_insert_deduplicate_m2m_strict_superset_exists(engine):
#     with sa_orm.Session(engine, expire_on_commit=False) as session:
#         cfg = init_hydra_cfg('Config', ['model.fields=[{power: 1},{power: 2}]'])
#         sc_ref = orm.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
#         session.commit()
#         cfg = init_hydra_cfg('Config', ['model.fields=[{power: 1}]'])
#         sc_new = orm.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
#
#         assert sc_ref != sc_new
