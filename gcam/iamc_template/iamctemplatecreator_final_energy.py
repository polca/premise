import pandas as pd
import numpy as np
from pathlib import Path
import os


def run_final_energy(scenario_name):
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # load LCI data. only one file
    final_energy = pd.read_csv(DATA_DIR / 'final energy by sector and fuel.csv')
    # hard coded mapping for sectors and inputs
    input_mapping = {
        'elect_td_ind': 'Electricity',
        'process heat dac': 'Heat',
        'H2 wholesale delivery': 'Hydrogen',
        'H2 wholesale dispensing': 'Hydrogen',
        'refined liquids industrial': 'Oil',
        'H2 industrial': 'Hydrogen',
        'oil-credits': 'Oil',
        'elect_td_bld': 'Electricity',
        'delivered coal': 'Coal',
        'refined liquids enduse': 'Oil',
        'H2 retail delivery': 'Hydrogen',
        'delivered biomass': 'Biomass',
        'delivered gas': 'Gas',
        'process heat food processing': 'Heat',
        'waste biomass for paper': 'Biomass',
        'wholesale gas': 'Gas',
        'global solar resource': 'Solar',
        'traditional biomass': 'Biomass',
        'elect_td_trn': 'Electricity',
        'H2 retail dispensing': 'Hydrogen',
        'regional woodpulp for energy': 'Biomass',
        'process heat paper': 'Heat',
        'district heat': 'District Heating'
    }
    print(final_energy['sector'].unique())


run_final_energy('ssp24p5tol5')