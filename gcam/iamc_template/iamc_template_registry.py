from iamctemplatecreator_biomass import run_biomass
from iamctemplatecreator_carbon_dioxide_removal import run_cdr
from iamctemplatecreator_cement import run_cement
from iamctemplatecreator_crops import run_crop
from iamctemplatecreator_electricity import run_electricity
from iamctemplatecreator_final_energy import run_final_energy
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


CREATOR_FUNCTIONS = [
    run_biomass,
    run_cdr,
    run_cement,
    run_crop,
    run_electricity,
    run_final_energy,
    run_fuel,
    run_heat,
    run_other,
    run_steel,
    run_bus,
    run_passenger_cars,
    run_rail_freight,
    run_road_freight,
    run_sea_freight,
    run_two_wheelers,
]