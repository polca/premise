
import pandas as pd
import yaml
from pathlib import Path
import os

def run_electricity(scenario_name):
   DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data for electricity. two files: 
    # - one with elec generation (in EJ)
    # - one with elec inputs (in EJ)
   elec_gen = pd.read_csv(DATA_DIR /'elec gen by gen tech.csv')
   elec_input = pd.read_csv(DATA_DIR /'elec energy input by elec gen tech.csv')

   print(elec_gen['technology'].unique())


run_electricity('ssp24p5tol5')