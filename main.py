import hydra
from omegaconf import OmegaConf

import cs


@hydra.main(version_base=None, config_name='config')
def main(cfg):
    print(OmegaConf.to_yaml(cfg, sort_keys=True))
    with cs.orm.Session(cs.engine) as session:
        cfg = hydra.utils.instantiate(cfg)
        breakpoint()
        session.add(cfg)
        session.commit()


if __name__ == "__main__":
    main()
