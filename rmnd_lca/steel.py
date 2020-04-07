import csv
from wurst import searching as ws
from wurst.transformations.utils import copy_dataset
from . import DATA_DIR
from .geomap import Geomap
from .data_collection import RemindDataCollection

from .utils import *


STEEL_TYPE_ACT = (DATA_DIR / "activity_type_steel_mapping.txt")
FIND_ACT_EI = (DATA_DIR / "fuel_type_activity_mapping.txt")
NAME_TO_LHV_NAME = (DATA_DIR / "name_to_lhv_name.txt")
PREF_REMIND_TO_EI_REGION = (DATA_DIR / "pref_location_remind_to_ei.txt")

class Steel:
    """
    Class that modifies steel markets in ecoinvent based on REMIND output data.

    :ivar scenario: name of a Remind scenario
    :vartype scenario: str
    
    """
    

    def __init__(self, db, rmd, scenario, year):
        self.db = db
        self.scenario = scenario
        self.rmd = rmd
        self.year = year
        self.fuels_lhv = get_lower_heating_values()
        self.geo = Geomap()
        #self.generate_activities = self.generate_activities()


    
    def get_abs_fuel(self, r, prim_sec, fuel_state, fuel_spec, logging=False):
        """
        This function gets the absolute fuel consumption for a specific fuel.
        For example, we have Solids but this can be made out of Coal, Biomass, Gas and/or Oil.        
        
        :param r: a REMIND region
        :param prim_sec: 'Primary' or 'Secondary' steel str
        :param fuel_state: type of fuel, i.e. "Solids","Liquids","Gases","Hydrogen" or "Heat" str
        :param fuel_spec: specific fuel in fuel mix including type, e.g., "Solids|Coal", "Solids|Biomass", "Solids|Traditional Biomass". str
        
        :return: value for fuel
        :rtype: float
        """

        # Get specific absolute value for the fuel state for steel production, specified for Primary or Secondary Steel 
        abs_fuel_steel = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                                if "FE|Industry" in v
                                                and fuel_state in v
                                                and "Electricity" not in v                
                                                and prim_sec in v], value=0)
               
        if (len(abs_fuel_steel) > 0 and fuel_state in str(fuel_spec)): 
            
            abs_fuel_steel = abs_fuel_steel.item()      
            
             # Define search string, which contains of the fuel|fuel_spec, e.g. SE|Solids|Coal
            string_to_find  = "SE|{}".format(fuel_spec)
                           
            # Get the specific secondary energy use
            abs_fuel_spec = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                                        if string_to_find == v], value=0)
            
            # If no value is found in the xarray, it could be that the fuel type is not in the array, fill with zero value       
            if len(abs_fuel_spec) < 1:
                if logging:
                    print("Combination not found for search string:'{}'".format(string_to_find))
                abs_fuel_spec = 0
            elif len(abs_fuel_spec) == 1:
                abs_fuel_spec = abs_fuel_spec.item()
            else:
                "Error: more than one value found in xarray for search string '{}'".format(string_to_find)
                                    
            # Finally, get the total secondary energy for the fuel we analyze
            string_to_find  = "SE|{}".format(fuel_state)   
            abs_fuel_total = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                                        if string_to_find == v], value=0).item()
            
            # Get absolute values compensated on shares in mix, hence divide specific SE fuel with total of SE and multiply with absolute steel value 
            rel_share = (abs_fuel_spec / abs_fuel_total)    
            abs_fuel_steel_spec = abs_fuel_steel * rel_share
            
        else:
            abs_fuel_steel_spec = 0
        
        return abs_fuel_steel_spec
        
    def get_rel_fuel(self, r, prim_sec, fuel_state, fuel_spec):
        """
        Gets relative value for a specific fuel (fuel_spec) in a fuel type (e.g. Solids) in total mix.
        
        :param r: a REMIND region
        :param prim_sec: 'Primary' or 'Secondary' steel str
        :param fuel_state: type of fuel, i.e. "Solids","Liquids","Gases","Hydrogen" or "Heat" str
        :param fuel_spec: specific fuel in fuel mix including type, e.g., "Solids|Coal", "Solids|Biomass", "Solids|Traditional Biomass". str
        
        :return: relative value for fuel
        :rtype: float
        """          
        energy_fuel = self.get_abs_fuel(r, prim_sec, fuel_state, fuel_spec, logging = False)
        
        energy_fuel_sum = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                            if "FE|Industry" in v
                                            and "Electricity" not in v
                                            and prim_sec in v], value=0).sum().item()
        
        # Select numeric columns and calculate the sums
        if energy_fuel != 0:
            energy_fuel_rel = energy_fuel / energy_fuel_sum
        else:
            energy_fuel_rel = 0

        return energy_fuel_rel
    
    def get_fuel_cons_without_elect(self, r, prim_sec, logging=False):
        """
        Gets data for fuel consumption in MJ/kg for primary and secondary steel WITHOUT considering electricity
        
        :param r: a REMIND region
        :param prim_sec: 'Primary' or 'Secondary' steel str
        
        :return: value for fuel consumption
        :rtype: float
        """          
        
        # Sum totals of fuel use, and convert from EJ to MJ (=1e12)
        sums_fe = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                            if "FE|Industry" in v
                                            and "Electricity" not in v
                                            and prim_sec in v], value=0).sum()*1e12 

        # Get total production volume, and convert from Mt to kg (=1e9)        
        volume_prod = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                            if "Production|Industry|Steel" in v
                                            and prim_sec in v], value=0).sum()*1e9
        
        # Now calculate fuel consumption in MJ/kg
        fuel_cons = sums_fe / volume_prod
        
        if (logging and fuel_cons.item() == 0):
            print("Warning: no energy consumption for '{}' steel found in year '{}'".format(prim_sec, self.year))
        
        # Prepare some logging in case needed
        if (logging and fuel_cons.item() > 0):
            print("The fuel consumption in year '{}' in region '{}' for the '{}' steel sector is '{}' [MJ/kg].".format(self.year,r,prim_sec,round(fuel_cons.item(),3)))        
        
        return fuel_cons.item()
    
    def get_fuel_cons_kg(self, r, prim_sec, fuel_state, fuel_spec, logging=False):
        """
        Gets data for fuel consumption in MJ/kg
        
        :param r: a REMIND region
        :param prim_sec: 'Primary' or 'Secondary' steel str
        :param fuel_state: type of fuel, i.e. "Solids","Liquids","Gases","Hydrogen" or "Heat" str
        :param fuel_spec: specific fuel in fuel mix including type, e.g., "Solids|Coal", "Solids|Biomass", "Solids|Traditional Biomass". str
        
        :return: value for fuel consumption
        :rtype: float
        """              
        fuel_cons_fuel = self.get_fuel_cons_without_elect(r, prim_sec, logging=False)
        relative_share_fuel = self.get_rel_fuel(r, prim_sec, fuel_state, fuel_spec)
        
        # In MJ/kg
        fuel_use_per_kg = relative_share_fuel * fuel_cons_fuel

        if (logging and fuel_use_per_kg > 0):
            print("The fuel consumption in year '{}' in region '{}' for the '{}' steel sector derived from fuel 'SE|{}' is '{}' [MJ/kg].".format(self.year,r,prim_sec,fuel_spec,round(fuel_use_per_kg,2)))        
                
        return fuel_use_per_kg
    
    def calculate_total_elect_cons_kwh(self, r, prim_sec):
        """
        This function calculates electricity consumption in kWh/kg steel production
        :param r: a REMIND region
        :param prim_sec: 'Primary' or 'Secondary' steel str
        
        :return: value for fuel consumption for electricity
        :rtype: float        
        """
        
        # Define search string for electricity consumption
        search_string = "FE|Industry|Electricity|Steel|{}".format(prim_sec)
        
        # Sum totals of fuel use, and convert from EJ to MJ (=1e12)
        sums_fe = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                            if v == search_string], value=0).item()*1e12
        
        # Define search string for production volume      
        search_string = "Production|Industry|Steel|{}".format(prim_sec)
        
        # Get total production volume, and convert from Mt to kg (=1e9)        
        volume_prod = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                            if v == search_string], value=0).item()*1e9

        # Calculate fuel consumption in MJ/kg, convert to kwh (factor 3.6)
        result = (sums_fe/volume_prod)/3.6

        return result
    
    def calculate_total_fuel_cons_kg(self, r, prim_sec, fuel_state, spec_fuel):
        """
        This function gets the total fuel consumption per kg of steel
        
        :param r: a REMIND region
        :param prim_sec: 'Primary' or 'Secondary' steel str
        :param fuel_state: list of all possible fuel states, e.g.: "Solids","Liquids","Gases","Hydrogen" or "Heat", list
        :param fuel_spec: specific fuel in fuel mix including type, e.g., "Solids|Coal", "Solids|Biomass", "Solids|Traditional Biomass". str
        
        :return: value for total fuel consumption per kg of steel
        :rtype: float          
        """
        count = 0
        
        # Loop over fuel states, count and sum
        for fs in fuel_state:
            result = self.get_fuel_cons_kg(r, prim_sec, fs, spec_fuel, logging=False)
            count += result
            
        return count
    
    def list_of_fuel_states(self, r, prim_sec):
        """
        This function gives back the value of specific fuel energy use in a region for Primary or Secondary steel     

        :param r: a REMIND region
        :param prim_sec: 'Primary' or 'Secondary' steel str

        :return: list of fuels consumed in region 'r' and chosen 'year'
        :rtype: list
        """   
        list_fuels = []
        
        # Get all fuels for fuel mix
        list_states = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                                if "FE" in v and
                                                prim_sec in v and
                                                not "Electricity" in v
                                                                     ], value=0)
        
        # Get only the variables/fuel where value is more than zero
        list_states = list_states.where(list_states.values > 0).dropna("variables")    
        list_fuels = list_states.coords["variables"].values.tolist()
        
        # Replace some substrings
        string_prim_sec = "|Steel|{}".format(prim_sec)
        list_fuels = [w.replace(string_prim_sec, "").replace("FE|Industry|", "") for w in list_fuels]

        return list_fuels  
    
    def list_of_unique_fuels_in_mix(self, r, prim_sec, fuel_state):
        """
        This function gives back the list of fuels in a region for Primary or Secondary steel
        per state of fuel, i.e. , e.g.: "Solids","Liquids","Gases","Hydrogen" or "Heat". 1 input per time.

        :param r: a REMIND region
        :param prim_sec: 'Primary' or 'Secondary' steel str
        :param prim_sec: state of fuel, e.g. "Solids","Liquids","Gases","Hydrogen", str

        :return: list of fuels consumed in region 'r' and chosen 'year'
        :rtype: list
        """             
        mix_list = []
        
        # Get all fuels for fuel mix
        mix_list = self.steel_xarray.sel(years=self.year, regions=r, variables=[v for v in self.steel_xarray.variables.values
                                                if fuel_state in v and
                                                v.count('|') == 2 and
                                                not "Fossil" in v and
                                                not "CHP" in v                                              
                                                                     ], value=0)
        
        # Get only the variables/fuel where value is more than zero
        mix_list = mix_list.where(mix_list.values > 0).dropna("variables")    
        list_fuels = mix_list.coords["variables"].values.tolist()
        
        # Replace some substrings
        list_fuels = [w.replace("SE|", "") for w in list_fuels]

        return list_fuels

    def list_regions_ei_act(self, act_name, reference_product):
        """
        This gives back a list with locations in ecoinvent fo a specific activity

        :param act_name: activity name, str
        :param reference_product: reference_product, str

        :return: list with ecoinvent locations of an activity
        """  
        
        list_to_sum = []

        # For the selected country
        for act in ws.get_many(
                    self.db,
                    ws.equals("name", act_name),
                    ws.equals("reference product", reference_product),
                    ws.contains("name", reference_product)
                    ):

            for exc in ws.production(act):
                list_to_sum.append(exc["location"])

        return list_to_sum
    
    def get_production_share_ei_region(self, act_name, reference_product, spec_ei_loc, ei_locs):
        """
        This function the share in the production volume of the dataset of all countries or selected number of countries

        :param act_name: activity name, str
        :param reference_product: reference_product, str
        :param countries: countries to include, list or str(i.e. "all")

        :return: share in production volume, float
        """  
        
        list_to_sum = []

        # For the selected country
        for act in ws.get_many(
                    self.db,
                    ws.equals("name", act_name),
                    ws.equals("reference product", reference_product),
                    ws.equals("location", spec_ei_loc)        
                    ):

            for exc in ws.production(act):
                prod_vol_spec = exc["production volume"]

        if ei_locs == "All":                     
            # Now get total over all countries   
            for act in ws.get_many(
                        self.db,
                        ws.equals("name", act_name),
                        ws.equals("reference product", reference_product),
                        ws.exclude(ws.contains("location", "GLO"))
                        ):

                for exc in ws.production(act):
                    list_to_sum.append(exc["production volume"])
        else:
            # Now get total over a selected number of countries
            for rg in ei_locs:
                for act in ws.get_many(
                            self.db,
                            ws.equals("name", act_name),
                            ws.equals("reference product", reference_product),
                            ws.equals("location", rg)              
                            ):

                    for exc in ws.production(act):
                        list_to_sum.append(exc["production volume"])        

        total = sum(list_to_sum)
        share = prod_vol_spec / total

        return share
    
    def define_new_exchanges(self, r, act_data, total, lhv, new_exchanges):
        """
        Adds exchanges to exchanges of an activity, furthermore it checks if the exchange is already available
        If there already is an identical exchange available, tehn it will get the old amount and add the new amount 
        to to the new exchanges and replaces the old one.
        
        :param r: region, str
        :param reference_product: act_data, type db input
        :param total: total amount which should be added in exchange, float
        :param lhv: lhv value for fuel, float        
        
        :return: nothing, adds exchanges
        
        """  
        # Define the string which is used if there is already an exchange with the same parameters
        search_string = "'name': '{}', 'product': '{}', 'unit': '{}', 'location': '{}'".format(act_data['name'],
                                                                            act_data['reference product'],
                                                                                        act_data['unit'],
                                                                                    act_data['location'])

        # We have to divide by energy density if obtained activity is in kilogram or cubic meter, since steel prod is usually in kg
        if act_data['unit'] == "kilogram" or act_data['unit'] == "cubic meter":
            # If activity for new exchange is in kilogram or cubic meter, we have to convert MJ to this specific unit
            amount = total / float(lhv)    
            
        # If activity is in MJ, we don't have to divide
        elif act_data['unit'] == "megajoule":
            amount = total
        else:
            print("Unit not recognized, please check unit: '{}'".format(act_data['unit']))

        # Check if the exchange has already be made or if we do have to make a new one
        if search_string not in str(new_exchanges):
            # Append exchange to exchanges of the specific activity for region r
            
            new_exchanges.append({
                        'name': act_data['name'],
                        'product': act_data['reference product'],
                        'unit': act_data['unit'],
                        'location': act_data['location'],
                        'amount': amount,
                        'type': 'technosphere',
                    })
            
        else:
            # There is already an exchange with the same name for the activity/location/ref product, add amount to this one                 
            indices = str([i for i, s in enumerate(new_exchanges) if act_data['name'] in str(s)
                                              and act_data['location'] in str(s)])

            # Get the exchange we are analyzing
            res = [exc for exc in new_exchanges if act_data['name'] in str(exc)
                   and act_data['location'] in str(exc)]

            # Get the string so we can modify it
            string_res = str(res)

            # Now select the "amount" data and add the amount to the initial amount
            amount_flt = float(string_res[(string_res.index("amount") + 3 + len("amount")):(string_res.index("type")-3)])
            amount_tot = amount_flt + amount  

            # Obtain the place/indice of the initial exchange, modify it a bit to an integer since it is in []
            indices = int(indices.replace("[","").replace("]",""))

            # Pop the old exchange which will be replaced with the new one, identically but with a different 'amount'
            new_exchanges.pop(indices)

            # Append the exchange to the exchanges
            new_exchanges.append(
                {
                    'name': act_data['name'],
                    'product': act_data['reference product'],
                    'unit': act_data['unit'],
                    'location': act_data['location'],
                    'amount': amount_tot,
                    'type': 'technosphere',
                }
                )
            
    def get_steel_activities(self):
        """
        This function gets all steel production activities in ecoinvent db and make a list of the available unique steel activities
        
        :param db: db 
        
        :return: list of unique steel actitivies
        :rtype: list
        """
        
        # Get all steel activities
        ds = ws.get_many(self.db,
                 ws.contains('name', 'steel production'),
                 ws.contains('reference product', 'steel'),
                 ws.exclude(ws.contains('name', 'market for')),
                 ws.exclude(ws.contains('name', 'reinforcing')),
                 ws.exclude(ws.contains('name', 'hot rolled'))                 
                )

        list_ecoinvent_processes = []
        
        # Append to list and select unique ones
        for d in ds:
            list_ecoinvent_processes.append(d['name'])
                  
        list_ecoinvent_processes = list(set(list_ecoinvent_processes))
        
        return list_ecoinvent_processes
    
    def delete_empty_exchanges(self, exchanges):
        """
        This function deletes all empty exchanges of a dict
        
        :param exchanges: dict with exchanges
        
        :return: dict without empty exchanges
        :rtype: dict       
        
        """   
        if not isinstance(exchanges, (dict, list)):
            return exchanges
        if isinstance(exchanges, list):
            return [v for v in (self.delete_empty_exchanges(v) for v in exchanges) if v]
        return {k: v for k, v in ((k, self.delete_empty_exchanges(v)) for k, v in exchanges.items()) if v}
        
    def generate_activities(self):
        """
        This function generates new activities and add them to the ecoinvent db.
        
        :return: NOTHING. Returns a modified database with newly added steel activities for the corresponding year
        
        Make new exchanges for each steel activity per region, the procedure is as follows:
        #1. Determine all steel activities in the ecoinvent db.
        #2. Loop over these steel activities.
            #3. Map and link ecoinvent regions to remind regions for steel activities.
            #4. Store copies of old steel activity.
            #5. Delete fuel exchanges and delete empty exchanges in copies.
            #6. Loop over Remind regions and add new steel activity for each Remind region.
                #7. First, get fuel states which are non-zero (e.g. Solids, Liquids, Gases..) and get all ecoinvent regions of a Remind region
                #8. Loop over fuel states and determine the secondary fuel mix (e.g. SE|Heat|Biomass, SE|Solids|Coal)
                #9. Next, loop over this fuel mix and determine the absolute amount of this fuel, if more than zero:
                #10. Make a new exchange for the steel activity. Determine the type of steel, linked ecoinvent activity, lhv and preferable ecoinvent region by using mapping.
                #11. Search for a suitable ecoinvent act. in the modified database. Now search for the most appropriate location:
                    #11.1. If only one suitable location found for Remind region, pick this region and make a new exchange for the steel activity.
                    #11.2. In case no suitable location is found for the exchange, pick the Global (GLO, RoW, ..) location. 
                    # In case of remind region Europe (EUR), choose 'RER' first, else 'Europe without Switzerland' or even 'GLO'.
                    #11.3. In case more than 1 activity found with a suitable location for the Remind region, we first remove the "GLO" location
                    # Since we have better and more specific location(s). Check if one suitable location is left, in case true pick this one and make a new exchange.
                    #11.4. If there is still more than 1 suitable location for the Remind region, we will determine the share of each activity location for the Remind region.
                    # We will do this making use of production volumes for each suitable ecoinvent location. Loop over the suitable ecoinvent activities with their location.
                    # Add a new exchange for each one, compensated for its production volume with a relative share. Add this activity as a new exchange to steel activity.
                #12. Furthermore, also add an exchange for electricity consumption. Link this to the electricity datasets made for each Remind region.
                #13. Now add the newly made exchanges to the steel activity dataset for Remind region r.
                #14. Add the new steel activity for Remind region r to the db.
        
        """   
        
        #1. Determine all steel activities in the db.
        ei_activities = self.get_steel_activities()
        count  = 0
        
        #2. Loop over the steel activities and generate new ones for each remind region
        for ei_act in ei_activities:
            
            count += 1
            print("Start generating new steel activities for steel activity: '{}' [{}/{}]".format(ei_act,count,len(ei_activities)))
            
            ds = ws.get_many(self.db,
                     ws.contains('name', ei_act),
                     ws.contains('reference product', 'steel'),
                     ws.exclude(ws.contains('name', 'market for')),
                     ws.exclude(ws.contains('name', 'reinforcing')),
                     ws.exclude(ws.contains('name', 'hot rolled'))                 
                    )

            list_steel_countries = [d["location"] for d in list(ds)]
            
            #3. Map and link ecoinvent to remind regions for steel activities. Use Geomap to identify and map regions
            d_map = {self.geo.ecoinvent_to_remind_location(d):d
                     for d in list_steel_countries}

            # Identify and make a list of Remind regions
            list_REMIND_regions = [c[1] for c in self.geo.geo.keys()
                           if type(c) == tuple
                           and c[0] == "REMIND"]

            # Link each available steel process to remind region, best match
            d_REM_to_eco = {r:d_map.get(r, "RoW") for r in list_REMIND_regions}


            d_act_steel = {}
            
            #4. Store copies of old steel activity., make sure that they have the unqique identifier for each region
            for d in d_REM_to_eco:
                ds = ws.get_one(self.db,
                             ws.contains('name', ei_act),
                             ws.equals('location', d_REM_to_eco[d]))
                d_act_steel[d] = copy_dataset(ds)
                d_act_steel[d]['location'] = d  
                
                
            #5. Delete fuel exchanges and delete empty exchanges. Fuel exchanges to remove:
            list_fuels = [
                        "diesel",
                        "coal",
                        "lignite",
                        "coke",
                        "fuel",
                        "meat",
                        "gas",
                        "oil",
                        "electricity",
                        ]
            
            # Function which makes sure to keep the exchanges without above fuels.
            keep = lambda x: {k: v for k, v in x.items()
                  if not any(ele in x["name"]
                         for ele in list_fuels)}
            
            # Determine if steel activity is primary or secondary steel
            #dict_type_steel = self.activity_to_steel_type_mapping()
            steel_type =self.determine_prim_sec_steel.get(d_act_steel[d]['name']) 
            
            #6. Loop over Remind regions for each steel activity
            for r in d_act_steel:
                # Modify exchanges, delete fuels in list_fuels. After that, delete empty exchanges
                d_act_steel[r]["exchanges"] = [keep(exc) for exc in d_act_steel[r]["exchanges"]]
                d_act_steel[r]["exchanges"] = self.delete_empty_exchanges(d_act_steel[r]["exchanges"])

                #7. Get fuel states which are non-zero
                fuel_states = self.list_of_fuel_states(r, steel_type)

                # Reset new exchanges
                new_exchanges = []

                # Determine possible ecoinvent locations in r
                ei_locations = self.geo.remind_to_ecoinvent_location(r)

                #8. Loop over fuel states and determine the secondary fuel mix
                for f_state in fuel_states:
                    fuel_mix = self.list_of_unique_fuels_in_mix(r, steel_type, f_state)

                    #9. Next, loop over this fuel mix and determine the absolute amount of this fuel
                    for fuel in fuel_mix:
                        total = 0
                        total = self.calculate_total_fuel_cons_kg(r,steel_type,fuel_states,fuel)

                        #10. Make a new exchange for the steel activity.
                        if total > 0:
                            # Get name, activity name and lhv value
                            act = self.fuel_type_to_lhv_mapping().get(fuel) 
                            act_name = self.fuel_type_to_activity_mapping().get(fuel) 
                            lhv = self.get_lower_heating_values().get(act)
                            pref_region = self.pref_remind_to_ei_region().get(r)

                            #11. Search for a suitable ecoinvent act.
                            act_data = [act for act in self.db
                                        if act['name'] == act_name and
                                        act['location'] in ei_locations and
                                        act['reference product'] in act['name']]

                            #11.1. If only one suitable location found for Remind region
                            if len(act_data) == 1:
                                act_data = act_data[0]
                                self.define_new_exchanges(r, act_data, total, lhv, new_exchanges)

                            #11.2. In case no suitable location is found for the exchange
                            elif len(act_data) == 0:
                                # In case of Europe, just select Europe as first, if not select GLO
                                if r == "EUR":
                                    act_data = [act for act in self.db
                                            if act['name'] == act_name and
                                            (act['location'] in ["RER","Europe without Switzerland","GLO","RoW"])]
                                    if len(act_data) > 0:
                                        act_data = act_data[0]
                                        self.define_new_exchanges(r, act_data, total, lhv, new_exchanges)
                                    else:
                                        print("activity not found for act '{}', region '{}' with fuel '{}'"
                                              .format(act_name,r,fuel.lower()))
                                # In this case, we just select GLO, in case not available, select RoW, RER.
                                # Add possibility to choose CH since this one is required for act Hydrogen|Gas (Hydrogen, gaseous, 25 bar, from SMR of natural gas)
                                else:
                                    act_data = [act for act in self.db
                                            if act['name'] == act_name and
                                            (act['location'] in ["GLO","RoW","RER","CH"]) 
                                           ]

                                    if len(act_data) > 0:
                                        act_data = act_data[0]
                                        self.define_new_exchanges(r, act_data, total, lhv, new_exchanges)
                                    else:
                                        print("activity not found for act '{}', region '{}' with fuel '{}'"
                                              .format(act_name,r,fuel))

                            #11.3. In case more than 1 activity found
                            else:
                                # Delete global location, since we have better and more specific location(s)
                                if "GLO" in str(ei_locations):
                                    ei_locations.remove("GLO")

                                # If we now have only one location left, we can choose this as the appropriate one
                                if len(ei_locations) == 1:
                                    act_data = [act for act in self.db
                                                if act['name'] == act_name and
                                                act['location'] in ei_locations and
                                                act['reference product'] in act['name']][0]               

                                    self.define_new_exchanges(r, act_data, total, lhv, new_exchanges)

                                #11.4. If there is still more than 1 suitable location for the Remind region
                                else:
                                    # select preferable region based on mapping
                                    act_data = [act for act in self.db
                                                if act['name'] == act_name and
                                                act['location'] == pref_region and
                                                act['reference product'] in act['name']]

                                    # If found, select the preferable region
                                    if len(act_data) == 1:
                                        act_data = act_data[0]
                                        self.define_new_exchanges(r, act_data, total, lhv, new_exchanges)

                                    # Still more than one dataset found, hence determine/compensate activities based on production volumes
                                    else:
                                        act_data = [act for act in self.db
                                                if act['name'] == act_name and
                                                act['location'] in ei_locations and
                                                act['reference product'] in act['name']]

                                        # Make a list to specify the share to be used.
                                        list_spec_locs = []

                                        # Get all locations found in the datasets
                                        for lc in act_data:
                                            list_spec_locs.append(lc['location'])

                                        # Loop over the dataset and add exchanges when the location is in ecoinvent
                                        for act in act_data:
                                            # Get all ecoinvent locations for the activity
                                            ei_locations_act = self.list_regions_ei_act(act_name, act['reference product'])

                                            # Only add an activity when it's in the ecoinvent locations and compensate for its share based on prod. vol.
                                            if act['location'] in ei_locations_act:
                                                #print(ei_locations_act, act['location'], act_name, act['name'], act['reference product']) 

                                                # Get specific activity
                                                act_spec = [ac for ac in self.db
                                                            if ac['name'] == act['name'] and
                                                            ac['location'] == act['location'] and
                                                            ac['reference product'] == act['reference product']]

                                                # Make new exchange
                                                if len(act_spec) == 1:
                                                    # Okay, we have found one activity, analyze and compensate for its prod. volume
                                                    act_spec = act_spec[0]

                                                    # Get the production share for the region
                                                    share_lc = self.get_production_share_ei_region(act_spec['name'], 
                                                                                                    act_spec['reference product'], 
                                                                                                    act_spec['location'], list_spec_locs)

                                                    # Multiply the share with the calculated total, to compensate for the prod. volume 
                                                    total = share_lc * total

                                                    # Add new exchanges one by one
                                                    self.define_new_exchanges(r, act_spec, total, lhv, new_exchanges)

                                                else:
                                                    print("Error: no activity found for activity '{}' with location '{}'".format(act_spec['name'], 
                                                                                                                           act_spec['location']))

                #12. Furthermore, also add an exchange for electricity consumption.
                amount = self.calculate_total_elect_cons_kwh(r, steel_type)

                if amount > 0:
                    # Match with new electricity market name after modification of electricity 
                    act_data = [act for act in self.db 
                                if act['name'] == "market group for electricity, medium voltage" and
                                act['location'] == r
                               ]

                    # Check if it is found, if not just take global mix for now              
                    if len(act_data) == 1:
                        act_data = act_data[0]
                    else:
                        # Activity not available with corresponding region, hence take the market global electricity mix
                        act_data = [act for act in self.db
                                if act['name'] == "market group for electricity, medium voltage" and
                                act['location'] == "GLO" and
                                act['reference product'] in act['name']][0]   

                    # Append exchange to exchanges of the specific activity for region r
                    new_exchanges.append(
                            {
                                'name': act_data['name'],
                                'product': act_data['reference product'],
                                'unit': act_data['unit'],
                                'location': act_data['location'],
                                'amount': amount,
                                'type': 'technosphere',
                            }
                            )

                #13. Now add the newly made exchanges to the steel activity dataset for Remind region r.
                if len(new_exchanges) > 0:     
                    d_act_steel[r]["exchanges"] += new_exchanges

                #14. Add the new steel activity for Remind region r to the db.

                self.db.append(d_act_steel[r])

