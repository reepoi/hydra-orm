import hydra
from omegaconf import OmegaConf

import cs


@hydra.main(version_base=None, config_name='config')
def main(cfg):
    print(OmegaConf.to_yaml(cfg, sort_keys=True))
    with cs.orm.Session(cs.engine) as session:
        sc = cs.instantiate_and_insert_config(session, OmegaConf.to_container(cfg))
        session.commit()
        breakpoint()
        print('end')


if __name__ == "__main__":
    main()
