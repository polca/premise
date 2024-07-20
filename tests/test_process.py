import os

import bw2data
import bw2io

ei_user = os.environ["EI_USERNAME"]
ei_pass = os.environ["EI_PASSWORD"]


def test_brightway():
    for ei_version in ["3.8", "3.9.1", "3.10"]:
        for system_model in ["cutoff", "consequential", "apos", "EN15804"]:

            bw2data.projects.set_current(f"ecoinvent-{ei_version}-{system_model}")

            if f"ecoinvent-{ei_version}-cutoff" not in bw2data.databases:
                bw2io.import_ecoinvent_release(
                    version=ei_version,
                    system_model="cutoff",
                    username=ei_user,
                    password=ei_pass,
                )

                print(bw2data.projects.current)
                print(bw2data.databases)
