import pandas as pd
import numpy as np
from pathlib import Path
import os

# code from Google AI

def sort_strings_then_numbers(item):
    """
    Key function to sort items with strings first, then numbers.
    """
    # Check if the item can be treated as a number (after potential conversion to string)
    is_number = str(item).isdigit()
    # Return a tuple for comparison:
    # (True, item) for numbers (which sorts after False)
    # (False, item) for strings (which sorts first)
    return (is_number, str(item))


# import iamctemplatecrator for each sector
from iamctemplatecreator_biomass import run_biomass
from iamctemplatecreator_carbon_dioxide_removal import run_cdr
from iamctemplatecreator_cement import run_cement
from iamctemplatecreator_crops import run_crop
from iamctemplatecreator_electricity import run_electricity
from iamctemplatecreator_fuel import run_fuel
from iamctemplatecreator_heat import run_heat
from iamctemplatecreator_other import run_other
from iamctemplatecreator_steel import run_steel
from iamctemplatecreator_transport_bus import run_bus
from iamctemplatecreator_transport_passenger_cars import run_passenger_cars
from iamctemplatecreator_transport_rail_freight import run_rail_freight
from iamctemplatecreator_transport_road_freight import run_road_freight
from iamctemplatecreator_transport_sea_freight import run_sea_freight
from iamctemplatecreator_transport_two_wheelers import run_two_wheelers

# set scenario
scenarios = ["ssp24p5tol5"]

for sc in scenarios:
	run_biomass(sc)
	run_cdr(sc)
	run_cement(sc)
	run_crop(sc)
	run_electricity(sc)
	run_fuel(sc)
	run_heat(sc)
	run_other(sc)
	run_steel(sc)
	run_bus(sc)
	run_passenger_cars(sc)
	run_rail_freight(sc)
	run_road_freight(sc)
	run_sea_freight(sc)
	run_two_wheelers(sc)

	excel_files = [f for f in os.listdir(os.path.join('..', 'output', sc)) if f.endswith('.xlsx')]
	# read in all excel files and combine into one database
	df_list = [pd.read_excel(os.path.join('..', 'output', sc, f)) for f in excel_files]
	out_df = pd.concat(df_list, axis=0)

	# order columns with number names after other columns
	out_df = out_df.reindex(sorted(out_df.columns, key=sort_strings_then_numbers), axis=1)
	# output
	out_df.to_excel(os.path.join('..', 'output', sc, f'iamc_template_gcam_{sc}.xlsx'), index=False)
