from collections import defaultdict
from pathlib import Path

import yaml

from premise import DATA_DIR

DIR_MAPS = DATA_DIR / "activities_mapping"


class TagLibrary:
    class _proxy:
        def __init__(self, collectdictref):
            self.collectdictref = collectdictref

        def __getitem__(self, item):
            return self.collectdictref[item]

    def __init__(self):
        self.__forward = {}
        self.__backward = defaultdict(list)
        self.get_activity = self._proxy(self.__forward)
        self.get_tag = self._proxy(self.__backward)

    def __contains__(self, item):
        return item in self.__backward

    def load(self):

        for ifile in Path(DIR_MAPS).glob("*.yaml"):
            with ifile.open() as ipf:

                self.__forward.update(yaml.load(ipf, yaml.SafeLoader))

        for v, k in self.__forward.items():
            for sub_key in k:
                self.__backward[sub_key].append(v)

        self.__backward = dict(self.__backward)

        return self

    def tags(self):
        return tuple(self.__forward.keys())

    def activities(self):
        return tuple(self.__backward.keys())


if __name__ == "__main__":
    t = TagLibrary()
    t.get_activity["Biomass CHP"]
