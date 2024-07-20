import os
import bw2io, bw2data

ei_user = os.environ["EI_USERNAME"]
ei_pass = os.environ["EI_PASSWORD"]

bw2data.projects.set_current("ei")

bw2io.import_ecoinvent_release(
    version="3.10",
    # other options are "consequential", "apos" and "EN15804"
    system_model="cutoff",
    username=ei_user,
    password=ei_pass,
    biosphere_name="biosphere"
)

def test_presence_database():
    assert "ecoinvent-3.10-cutoff" in bw2io.databases
    print(bw2io.databases)

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
