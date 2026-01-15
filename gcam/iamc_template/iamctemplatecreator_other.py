import pandas as pd
import numpy as np
from pathlib import Path
import os, re


def run_other(scenario_name):
    DATA_DIR = Path(os.path.join('..', 'queries', 'queryresults', scenario_name))

    # read in four files
    population = pd.read_csv(DATA_DIR / 'population by region.csv')
    gdp = pd.read_csv(DATA_DIR / 'GDP per capita PPP by region.csv')
    gmst = pd.read_csv(DATA_DIR / 'global mean temperature.csv')
    co2 = pd.read_csv(DATA_DIR / 'CO2 emissions by region.csv')

    print(population)

run_other('ssp24p5tol5')