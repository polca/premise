# python iamctemplatecreator_elec
# python iamctemplatecreator_steel
# python iamctemplatecreator_cement
# python iamctemplatecreator_fuel
# python iamctemplatecreator_biofuel
# python iamctemplatecreator_hydrogen
# python iamctemplatecreator_gas.py
# python iamctemplatecreator_cropLUC.py

import pandas as pd
import numpy as np
from pathlib import Path

# import iamctemplatecrator for each sectors
# from iamctemplatecreator_cropLUC import run_cropLUC
# from iamctemplatecreator_gas import run_gas
# from iamctemplatecreator_hydrogen import run_hydrogen
# from iamctemplatecreator_biofuel import run_biofuel
# from iamctemplatecreator_fuel import run_fuel
# from iamctemplatecreator_cement import run_cement
# from iamctemplatecreator_steel import run_steel
# from iamctemplatecreator_elec import run_elec
from iamctemplatecreator_scenario import run_scenario

# set scenario
scenarios = ["SSP2RCP26"]

for sc in scenarios:
	# run_cropLUC(sc)
	# run_gas(sc)
	# run_hydrogen(sc)
	# run_biofuel(sc)
	# run_fuel(sc)
	# run_cement(sc)
	# run_steel(sc)
	# run_elec(sc)
	run_scenario(sc)
