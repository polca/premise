import os

import bw2io

ei_user = os.environ["EI_USERNAME"]
ei_pass = os.environ["EI_PASSWORD"]

bw2io.projects.set_current("ei")

bw2io.import_ecoinvent_release(
    version="3.10",
    system_model="cutoff",  # other options are "consequential", "apos" and "EN15804"
    username=ei_user,
    password=ei_pass,
    biosphere_name="biosphere",
)


def test_presence_database():
    assert "ecoinvent-3.10-cutoff" in bw2io.databases
    print(bw2io.databases)
