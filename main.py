import hydra
from omegaconf import OmegaConf

import config


@hydra.main(version_base=None, config_name='config')
def main(cfg):
    print(OmegaConf.to_yaml(cfg, sort_keys=True))
    with config.orm.Session(config.engine) as session:
        cfg = hydra.utils.instantiate(cfg)
        session.add(cfg)
        session.commit()


if __name__ == "__main__":
    main()
