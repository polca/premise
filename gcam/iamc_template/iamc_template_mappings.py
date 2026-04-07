import pandas as pd


HEAT_FUEL_MAPPING = {
    "delivered coal": "Coal",
    "refined liquids enduse": "Oil",
    "H2 industrial": "Hydrogen",
    "delivered biomass": "Biomass",
    "refined liquids industrial": "Oil",
    "wholesale gas": "Gas",
    "elect_td_ind": "Electricity",
    "global solar resource": "Solar",
    "traditional biomass": "Biomass",
    "H2 retail delivery": "Hydrogen",
    "delivered gas": "Gas",
    "elect_td_bld": "Electricity",
    "district heat": "District Heat",
}


ELECTRICITY_TECH_MAPPING = {
    "biomass (IGCC CCS)": "Biomass IGCC CCS",
    "biomass (IGCC)": "Biomass IGCC",
    "biomass (conv CCS)": "Biomass ST CCS",
    "biomass (conv)": "Biomass ST",
    "coal (IGCC CCS)": "Coal IGCC CCS",
    "coal (IGCC)": "Coal IGCC",
    "coal (conv pul CCS)": "Coal PC CCS",
    "coal (conv pul)": "Coal PC",
    "gas (CC CCS)": "Gas CC CCS",
    "gas (CC)": "Gas CC",
    "gas (steam/CT)": "Gas ST",
    "geothermal": "Geothermal",
    "hydro": "Hydro",
    "Gen_III": "Nuclear",
    "refined liquids (CC CCS)": "Oil CC CCS",
    "refined liquids (CC)": "Oil CC",
    "refined liquids (steam/CT)": "Oil ST",
    "rooftop_pv": "Solar PV Residential",
    "CSP_storage": pd.NA,
    "PV": "Solar PV Centralized",
    "PV_storage": pd.NA,
    "wind": "Wind Onshore",
    "wind_offshore": "Wind Offshore",
    "wind_storage": pd.NA,
    "CSP": "Solar CSP",
    "Gen_II_LWR": "Nuclear",
}


FUEL_LIQUIDS_TECH_MAPPING = {
    "BTL with hydrogen": "BTL Hydrogen",
    "FT biofuels": "FT Biodiesel",
    "FT biofuels CCS level 1": "FT Biodiesel CCS",
    "FT biofuels CCS level 2": "FT Biodiesel CCS",
    "biodiesel": "Biodiesel",
    "cellulosic ethanol": "Cellulosic Ethanol",
    "cellulosic ethanol CCS level 1": "Cellulosic Ethanol CCS",
    "cellulosic ethanol CCS level 2": "Cellulosic Ethanol CCS",
    "corn ethanol": "Corn Ethanol",
    "coal to liquids": "Coal to Liquids",
    "coal to liquids CCS level 1": "Coal to Liquids CCS",
    "coal to liquids CCS level 2": "Coal to Liquids CCS",
    "gas to liquids": "Gas to Liquids",
    "oil refining": "Oil",
    "sugar cane ethanol": "Sugar Cane Ethanol",
}


FUEL_GAS_TECH_MAPPING = {
    "biomass gasification": "Biomass",
    "coal gasification": "Coal",
    "natural gas": "Gas",
}


FUEL_HYDROGEN_TECH_MAPPING = {
    "biomass to H2": "Biomass",
    "biomass to H2 CCS": "Biomass CCS",
    "coal chemical CCS": "Coal CCS",
    "electrolysis": "Electrolysis",
    "gas ATR CCS": pd.NA,
    "natural gas steam reforming": "Gas",
}


CEMENT_TECH_MAPPING = {
    "cement": "Cement",
    "cement CCS": "Cement CCS",
}


INDUSTRY_INPUT_MAPPING = {
    "delivered coal": "Coal",
    "elect_td_ind": "Electricity",
    "refined liquids industrial": "Refined Liquids",
    "wholesale gas": "Gas",
    "delivered biomass": "Biomass",
}


CROP_MAPPING = {
    "CornC4": "Maize",
    "SugarCropC4": "Sugar",
    "OilCrop": "Oilcrops",
    "biomassTree": "Wood",
    "biomassGrass": "Grass",
}


CDR_TECH_MAPPING = {
    "hightemp DAC NG": "Solvent",
    "hightemp DAC elec": "Solvent",
    "lowtemp DAC heatpump": "Sorbent",
}

FINAL_ENERGY_INPUT_MAPPING = {
    "elect_td_bld": "Electricity",
    "delivered coal": "Coal",
    "refined liquids enduse": "Oil",
    "H2 retail delivery": "Hydrogen",
    "delivered biomass": "Biomass",
    "delivered gas": "Gas",
    "traditional biomass": "Biomass",
    "district heat": "District Heat",
    "elect_td_ind": "Electricity",
    "H2 wholesale delivery": "Hydrogen",
    "H2 wholesale dispensing": "Hydrogen",
    "refined liquids industrial": "Oil",
    "H2 industrial": "Hydrogen",
    "wholesale gas": "Gas",
    "global solar resource": "Solar",
    "regional woodpulp for energy": "Biomass",
    "elect_td_trn": "Electricity",
    "H2 retail dispensing": "Hydrogen",
}

FINAL_ENERGY_BUILDINGS_SECTOR_MAPPING = {
    'comm cooling': "Commercial Cooling",
    'comm heating': "Commercial Heating",
    'comm others': "Commercial Other",
    'resid cooling modern_d1': "Residential Cooling",
    'resid cooling modern_d2': "Residential Cooling",
    'resid cooling modern_d3': "Residential Cooling",
    'resid cooling modern_d4': "Residential Cooling",
    'resid cooling modern_d5': "Residential Cooling",
    'resid cooling modern_d6': "Residential Cooling",
    'resid cooling modern_d7': "Residential Cooling",
    'resid cooling modern_d8': "Residential Cooling",
    'resid cooling modern_d9': "Residential Cooling",
    'resid cooling modern_d10': "Residential Cooling",
    'resid heating TradBio_d1': "Residential Heating",
    'resid heating TradBio_d2': "Residential Heating",
    'resid heating TradBio_d3': "Residential Heating",
    'resid heating TradBio_d4': "Residential Heating",
    'resid heating TradBio_d5': "Residential Heating",
    'resid heating TradBio_d6': "Residential Heating",
    'resid heating TradBio_d7': "Residential Heating",
    'resid heating TradBio_d8': "Residential Heating",
    'resid heating TradBio_d9': "Residential Heating",
    'resid heating TradBio_d10': "Residential Heating",
    'resid heating modern_d1': "Residential Heating",
    'resid heating modern_d2': "Residential Heating",
    'resid heating modern_d3': "Residential Heating",
    'resid heating modern_d4': "Residential Heating",
    'resid heating modern_d5': "Residential Heating",
    'resid heating modern_d6': "Residential Heating",
    'resid heating modern_d7': "Residential Heating",
    'resid heating modern_d8': "Residential Heating",
    'resid heating modern_d9': "Residential Heating",
    'resid heating modern_d10': "Residential Heating",
    'resid others TradBio_d1': "Residential Other",
    'resid others TradBio_d2': "Residential Other",
    'resid others TradBio_d3': "Residential Other",
    'resid others TradBio_d4': "Residential Other",
    'resid others TradBio_d5': "Residential Other",
    'resid others TradBio_d6': "Residential Other",
    'resid others TradBio_d7': "Residential Other",
    'resid others TradBio_d8': "Residential Other",
    'resid others TradBio_d9': "Residential Other",
    'resid others TradBio_d10': "Residential Other",
    'resid others coal_d1': "Residential Other",
    'resid others coal_d2': "Residential Other",
    'resid others coal_d3': "Residential Other",
    'resid others coal_d4': "Residential Other",
    'resid others coal_d5': "Residential Other",
    'resid others coal_d6': "Residential Other",
    'resid others coal_d7': "Residential Other",
    'resid others coal_d8': "Residential Other",
    'resid others coal_d9': "Residential Other",
    'resid others coal_d10': "Residential Other",
    'resid others modern_d1': "Residential Other",
    'resid others modern_d2': "Residential Other",
    'resid others modern_d3': "Residential Other",
    'resid others modern_d4': "Residential Other",
    'resid others modern_d5': "Residential Other",
    'resid others modern_d6': "Residential Other",
    'resid others modern_d7': "Residential Other",
    'resid others modern_d8': "Residential Other",
    'resid others modern_d9': "Residential Other",
    'resid others modern_d10': "Residential Other",
    'resid heating coal_d1': "Residential Heating",
    'resid heating coal_d2': "Residential Heating",
    'resid heating coal_d3': "Residential Heating",
    'resid heating coal_d4': "Residential Heating",
    'resid heating coal_d5': "Residential Heating",
    'resid heating coal_d6': "Residential Heating",
    'resid heating coal_d7': "Residential Heating",
    'resid heating coal_d8': "Residential Heating",
    'resid heating coal_d9': "Residential Heating",
    'resid heating coal_d10': "Residential Heating",
}

FINAL_ENERGY_INDUSTRY_SECTOR_MAPPING = {
    'CO2 removal': 'CDR',
    'agricultural energy use': 'Agriculture',
    'ammonia': 'Ammonia',
    'cement': 'Cement',
    'chemical energy use': 'Chemicals',
    'chemical feedstocks': pd.NA,
    'construction energy use': 'Construction',
    'construction feedstocks': pd.NA,
    'food processing': 'Food Processing',
    'iron and steel': 'Iron and Steel',
    'mining energy use': 'Mining',
    'other industrial energy use': 'Other Industry',
    'other industrial feedstocks': pd.NA,
    'process heat cement': 'Cement',
    'process heat dac': 'CDR',
    'process heat food processing': 'Food Processing',
    'waste biomass for paper': 'Pulp and Paper',
    'alumina': 'Aluminum',
    'aluminum': 'Aluminum',
    'paper': 'Pulp and Paper',
    'process heat paper': 'Pulp and Paper',
}

FINAL_ENERGY_INDUSTRY_SUBSECTOR_MAPPING = {
    # only iron and steel have unique subsectors in GCAM; rest should be mapped to NA
    'BLASTFUR': 'BF/BOF',
    'EAF with DRI': 'EAF DRI',
    'EAF with scrap': 'EAF Scrap',
    'dac': pd.NA,
    'mobile': pd.NA,
    'stationary': pd.NA,
    'refined liquids': pd.NA,
    'cement': pd.NA,
    'hydrogen': pd.NA,
    'food processing': pd.NA,
    'biomass': pd.NA,
    'coal': pd.NA,
    'electricity': pd.NA,
    'gas CCS': pd.NA,
    'gas': pd.NA,
    'heat': pd.NA,
    'district heat': pd.NA,
    'paper': pd.NA,
}

FINAL_ENERGY_TRANSPORT_SECTOR_MAPPING = {
    'trn_aviation_intl': 'Aviation',
    'trn_freight': 'Freight',
    'trn_freight_road': 'Freight',
    'trn_pass': 'Passenger',
    'trn_pass_road': 'Passenger',
    'trn_pass_road_LDV': 'Passenger',
    'trn_pass_road_LDV_4W': 'Passenger',
    'trn_shipping_intl': 'Shipping',
}