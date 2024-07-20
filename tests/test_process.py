import os
from pathlib import Path
from premise import NewDatabase
from ecoinvent_interface import Settings, EcoinventRelease, ReleaseType

ei_user = os.environ["EI_USERNAME"]
ei_pass = os.environ["EI_PASSWORD"]

my_settings = Settings(username=ei_user, password=ei_pass)

ei = EcoinventRelease(my_settings)

path = ei.get_release(
    version='3.7.1',
    system_model='apos',
    release_type=ReleaseType.ecospold
)
def test_check_file_existence():
    if not os.path.exists(path):
        assert False, f"File not found: {path}"
    else:
        # print the list of files in the directory
        print(os.listdir(path))
