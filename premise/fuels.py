from .transformation import *
from .utils import DATA_DIR, get_land_use_change_CO2_for_crops, get_land_use_for_crops

CROP_CLIMATE_MAP = DATA_DIR / "fuels" / "crop_climate_mapping.csv"
REGION_CLIMATE_MAP = DATA_DIR / "fuels" / "region_climate_mapping.csv"
FUEL_LABELS = DATA_DIR / "fuels" / "fuel_labels.csv"


def filter_results(item_to_look_for, results, field_to_look_at):
    return [r for r in results if item_to_look_for not in r[field_to_look_at]]


def get_crop_climate_mapping():
    """ Returns a dictionnary thatindictes the type of crop
    used for bioethanol production per type of climate """

    d = {}
    with open(CROP_CLIMATE_MAP, encoding="utf-8") as f:
        r = csv.reader(f, delimiter=";")
        next(r)
        for line in r:
            climate, sugar, oil, wood, grass, grain = line
            d[climate] = {
                "sugar": sugar,
                "oil": oil,
                "wood": wood,
                "grass": grass,
                "grain": grain,
            }
    return d


def get_region_climate_mapping():
    """ Returns a dicitonnary that indicates the type of climate
     for each IAM region"""

    d = {}
    with open(REGION_CLIMATE_MAP, encoding="utf-8") as f:
        r = csv.reader(f, delimiter=";")
        next(r)
        for line in r:
            region, climate = line
            d[region] = climate
    return d


def get_compression_effort(p_in, p_out, flow_rate):
    """ Calculate the required electricity consumption from the compressor given
    an inlet and outlet pressure and a flow rate for hydrogen. """

    # result is shaft power [kW] and compressor size [kW]
    # flow_rate = mass flow rate (kg/day)
    # p_in =  input pressure (bar)
    # p_out =  output pressure (bar)
    Z_factor = 1.03198  # the hydrogen compressibility factor
    N_stages = 2  # the number of compressor stages (assumed to be 2 for this work)
    t_inlet = 310.95  # K the inlet temperature of the compressor
    y_ratio = 1.4  # the ratio of specific heats
    M_h2 = 2.15  # g/mol the molecular mass of hydrogen
    eff_comp = 0.75  # %
    R_constant = 8.314  # J/(mol*K)
    part_1 = (
        (flow_rate * (1 / (24 * 3600)))
        * ((Z_factor * t_inlet * R_constant) / (M_h2 * eff_comp))
        * ((N_stages * y_ratio / (y_ratio - 1)))
    )
    part_2 = ((p_out / p_in) ** ((y_ratio - 1) / (N_stages * y_ratio))) - 1
    power_req = part_1 * part_2
    return power_req * 24 / flow_rate


def get_pre_cooling_energy(year, t_amb, cap_util):

    el_pre_cooling = (0.3 / 1.6 * np.exp(-0.018 * t_amb)) + (
        (25 * np.log(t_amb) - 21) / cap_util
    )

    return el_pre_cooling


class Fuels(BaseTransformation):
    """
        Class that modifies fuel inventories and markets in ecoinvent based on IAM output data.

        :ivar scenario: name of an IAM pathway
        :vartype pathway: str

    """

    def __init__(self, database, iam_data, model, pathway, year, version, original_db):
        super().__init__(database, iam_data, model, pathway, year)
        self.version = version
        self.original_db = original_db
        self.fuels_lhv = get_lower_heating_values()
        self.fuel_labels = self.iam_data.fuel_markets.coords["variables"].values
        self.fuel_co2 = get_fuel_co2_emission_factors()
        self.land_use_per_crop_type = get_land_use_for_crops(model=self.model)
        self.land_use_change_CO2_per_crop_type = get_land_use_change_CO2_for_crops(
            model=self.model
        )
        self.new_fuel_markets = {}

    def generate_dac_activities(self):

        """ Generate regional variants of the DAC process with varying heat sources """

        # define heat sources
        heat_map_ds = {
            "waste heat": (
                "heat, from municipal waste incineration to generic market for heat district or industrial, other than natural gas",
                "heat, district or industrial, other than natural gas",
            ),
            "industrial steam heat": (
                "market for heat, from steam, in chemical industry",
                "heat, from steam, in chemical industry",
            ),
            "heat pump heat": (
                "market group for electricity, low voltage",
                "electricity, low voltage",
            ),
        }

        # loop through IAM regions
        for region in self.regions:
            for heat_type, activities in heat_map_ds.items():

                dataset = wt.copy_to_new_location(
                    ws.get_one(
                        self.original_db,
                        ws.contains("name", "carbon dioxide, captured from atmosphere"),
                    ),
                    region,
                )

                new_name = f"{dataset['name']}, with {heat_type}, and grid electricity"

                dataset["name"] = new_name

                for exc in ws.production(dataset):
                    exc["name"] = new_name
                    if "input" in exc:
                        exc.pop("input")

                for exc in ws.technosphere(dataset):
                    if "heat" in exc["name"]:
                        exc["name"] = activities[0]
                        exc["product"] = activities[1]
                        exc["location"] = "RoW"

                        if heat_type == "heat pump heat":
                            exc["unit"] = "kilowatt hour"
                            exc["amount"] *= 1 / (
                                2.9 * 3.6
                            )  # COP of 2.9 and MJ --> kWh
                            exc["location"] = "RER"

                            dataset[
                                "comment"
                            ] = "Dataset generated by `premise`, initially based on Terlouw et al. 2021. "
                            dataset["comment"] += (
                                "A CoP of 2.9 is assumed for the heat pump. But the heat pump itself is not"
                                + " considered here. "
                            )

                dataset["comment"] += (
                    "The CO2 is compressed from 1 bar to 25 bar, "
                    + " for which 0.78 kWh is considered. Furthermore, there's a 2.1% loss on site"
                    + " and only a 1 km long pipeline transport."
                )

                dataset = relink_technosphere_exchanges(
                    dataset, self.database, self.model, contained=False
                )
                self.database.append(dataset)

                # Add created dataset to `self.list_datasets`
                self.list_datasets.append(
                    (dataset["name"], dataset["reference product"], dataset["location"])
                )

    def find_transport_activity(self, items_to_look_for, items_to_exclude, loc):

        try:
            dataset = ws.get_one(
                self.database,
                *[ws.contains("name", i) for i in items_to_look_for],
                ws.doesnt_contain_any("name", items_to_exclude),
                ws.equals("location", loc),
            )
        except ws.NoResults:
            dataset = ws.get_one(
                self.database,
                *[ws.contains("name", i) for i in items_to_look_for],
                ws.doesnt_contain_any("name", items_to_exclude),
            )

        return (
            dataset["name"],
            dataset["reference product"],
            dataset["unit"],
            dataset["location"],
        )

    def generate_hydrogen_activities(self):
        """

        Defines regional variants for hydrogen production, but also different supply
        chain designs:
        * by truck (100, 200, 500 and 1000 km), gaseous, liquid and LOHC
        * by reassigned CNG pipeline (100, 200, 500 and 1000 km), gaseous, with and without inhibitors
        * by dedicated H2 pipeline (100, 200, 500 and 1000 km), gaseous
        * by ship, liquid (1000, 2000, 5000 km)

        For truck and pipeline supply chains, we assume a transmission and a distribution part, for which
        we have specific pipeline designs. We also assume a means for regional storage in between (salt cavern).
        We apply distance-based losses along the way.

        Most of these supply chain design options are based on the work:
        * Wulf C, Reuß M, Grube T, Zapp P, Robinius M, Hake JF, et al.
          Life Cycle Assessment of hydrogen transport and distribution options.
          J Clean Prod 2018;199:431–43. https://doi.org/10.1016/j.jclepro.2018.07.180.
        * Hank C, Sternberg A, Köppel N, Holst M, Smolinka T, Schaadt A, et al.
          Energy efficiency and economic assessment of imported energy carriers based on renewable electricity.
          Sustain Energy Fuels 2020;4:2256–73. https://doi.org/10.1039/d0se00067a.
        * Petitpas G. Boil-off losses along the LH2 pathway. US Dep Energy Off Sci Tech Inf 2018.

        We also assume efficiency gains over time for the PEM electrolysis process: from 58 kWh/kg H2 in 2010,
        down to 44 kWh by 2050, according to a literature review conducted by the Paul Scherrer Institut.

        """

        fuel_activities = {
            "hydrogen": [
                (
                    "hydrogen production, gaseous, 25 bar, from electrolysis",
                    "from electrolysis",
                ),
                (
                    "hydrogen production, steam methane reforming, from biomethane, high and low temperature, with CCS (MDEA, 98% eff.), 26 bar",
                    "from SMR of biogas, with CCS",
                ),
                (
                    "hydrogen production, steam methane reforming, from biomethane, high and low temperature, 26 bar",
                    "from SMR of biogas",
                ),
                (
                    "hydrogen production, auto-thermal reforming, from biomethane, 25 bar",
                    "from ATR of biogas",
                ),
                (
                    "hydrogen production, auto-thermal reforming, from biomethane, with CCS (MDEA, 98% eff.), 25 bar",
                    "from ATR of biogas, with CCS",
                ),
                (
                    "hydrogen production, steam methane reforming of natural gas, 25 bar",
                    "from SMR of nat. gas",
                ),
                (
                    "hydrogen production, steam methane reforming of natural gas, with CCS (MDEA, 98% eff.), 25 bar",
                    "from SMR of nat. gas, with CCS",
                ),
                (
                    "hydrogen production, auto-thermal reforming of natural gas, 25 bar",
                    "from ATR of nat. gas",
                ),
                (
                    "hydrogen production, auto-thermal reforming of natural gas, with CCS (MDEA, 98% eff.), 25 bar",
                    "from ATR of nat. gas, with CCS",
                ),
                (
                    "hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass with CCS, at gasification plant",
                    "from gasification of biomass by heatpipe reformer, with CCS",
                ),
                (
                    "hydrogen production, gaseous, 25 bar, from heatpipe reformer gasification of woody biomass, at gasification plant",
                    "from gasification of biomass by heatpipe reformer",
                ),
                (
                    "hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, with CCS, at gasification plant",
                    "from gasification of biomass, with CCS",
                ),
                (
                    "hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, at gasification plant",
                    "from gasification of biomass",
                ),
                (
                    "hydrogen production, gaseous, 30 bar, from hard coal gasification and reforming, at coal gasification plant",
                    "from coal gasification",
                ),
            ]
        }

        for region in self.regions:
            for fuel, activities in fuel_activities.items():
                for act in activities:

                    dataset = wt.copy_to_new_location(
                        ws.get_one(self.original_db, ws.contains("name", act[0])),
                        region,
                    )

                    for exc in ws.production(dataset):
                        if "input" in exc:
                            exc.pop("input")

                    # we adjust the electrolysis efficiency
                    # from 58 kWh/kg H2 in 2010, down to 44 kWh in 2050
                    if (
                        act[0]
                        == "hydrogen production, gaseous, 25 bar, from electrolysis"
                    ):
                        for exc in ws.technosphere(dataset):
                            if "market group for electricity" in exc["name"]:
                                exc["amount"] = -0.3538 * (self.year - 2010) + 58.589

                        string = f" The electricity input per kg of H2 has been adapted to the year {self.year}."
                        if "comment" in dataset:
                            dataset["comment"] += string
                        else:
                            dataset["comment"] = string

                    dataset = relink_technosphere_exchanges(
                        dataset, self.database, self.model
                    )

                    dataset[
                        "comment"
                    ] = "Region-specific hydrogen production dataset generated by `premise`. "

                    self.database.append(dataset)
                    # Add created dataset to `self.list_datasets`
                    self.list_datasets.append(
                        (
                            dataset["name"],
                            dataset["reference product"],
                            dataset["location"],
                        )
                    )

        print("Generate region-specific hydrogen supply chains.")
        # loss coefficients for hydrogen supply
        losses = {
            "truck": {
                "gaseous": (
                    lambda d: 0.005,  # compression, per operation,
                    " 0.5% loss during compression.",
                ),
                "liquid": (
                    lambda d: (
                        0.013  # liquefaction, per operation
                        + 0.02  # vaporization, per operation
                        + np.power(1.002, d / 50 / 24)
                        - 1  # boil-off, per day, 50 km/h on average
                    ),
                    "1.3% loss during liquefaction. Boil-off loss of 0.2% per day of truck driving. "
                    "2% loss caused by vaporization during tank filling at fuelling station.",
                ),
                "liquid organic compound": (
                    lambda d: 0.005,
                    "0.5% loss during hydrogenation.",
                ),
            },
            "ship": {
                "liquid": (
                    lambda d: (
                        0.013  # liquefaction, per operation
                        + 0.02  # vaporization, per operation
                        + np.power(
                            0.2, d / 36 / 24
                        )  # boil-off, per day, 36 km/h on average
                    ),
                    "1.3% loss during liquefaction. Boil-off loss of 0.2% per day of shipping. "
                    "2% loss caused by vaporization during tank filling at fuelling station.",
                ),
            },
            "H2 pipeline": {
                "gaseous": (
                    lambda d: (
                        0.005  # compression, per operation
                        + 0.023  # storage, unused buffer gas
                        + 0.01  # storage, yearly leakage rate
                        + 4e-5 * d  # pipeline leakage, per km
                    ),
                    "0.5% loss during compression. 3.3% loss at regional storage."
                    "Leakage rate of 4e-5 kg H2 per km of pipeline.",
                )
            },
            "CNG pipeline": {
                "gaseous": (
                    lambda d: (
                        0.005  # compression, per operation
                        + 0.023  # storage, unused buffer gas
                        + 0.01  # storage, yearly leakage rate
                        + 4e-5 * d  # pipeline leakage, per km
                        + 0.07  # purification, per operation
                    ),
                    "0.5% loss during compression. 3.3% loss at regional storage."
                    "Leakage rate of 4e-5 kg H2 per km of pipeline. 7% loss during sepration of H2"
                    "from inhibitor gas.",
                )
            },
        }

        supply_chain_scenarios = {
            "truck": {
                "type": [
                    (
                        "market for transport, freight, lorry, unspecified",
                        "transport, freight, lorry, unspecified",
                        "ton kilometer",
                        "RER",
                    )
                ],
                "state": ["gaseous", "liquid", "liquid organic compound"],
                "distance": [
                    500,
                    # 1000
                ],
            },
            "ship": {
                "type": [
                    self.find_transport_activity(
                        items_to_look_for=[
                            "market for transport, freight, sea",
                            "liquefied",
                        ],
                        items_to_exclude=["other"],
                        loc="RoW",
                    )
                ],
                "state": ["liquid"],
                "distance": [
                    2000,
                    # 5000
                ],
            },
            "H2 pipeline": {
                "type": [
                    (
                        "distribution pipeline for hydrogen, dedicated hydrogen pipeline",
                        "pipeline, for hydrogen distribution",
                        "kilometer",
                        "RER",
                    ),
                    (
                        "transmission pipeline for hydrogen, dedicated hydrogen pipeline",
                        "pipeline, for hydrogen transmission",
                        "kilometer",
                        "RER",
                    ),
                ],
                "state": ["gaseous"],
                "distance": [
                    500,
                    # 1000
                ],
                "regional storage": (
                    "geological hydrogen storage",
                    "hydrogen storage",
                    "kilogram",
                    "RER",
                ),
                "lifetime": 40 * 400000 * 1e3,
            },
            "CNG pipeline": {
                "type": [
                    (
                        "distribution pipeline for hydrogen, reassigned CNG pipeline",
                        "pipeline, for hydrogen distribution",
                        "kilometer",
                        "RER",
                    ),
                    (
                        "transmission pipeline for hydrogen, reassigned CNG pipeline",
                        "pipeline, for hydrogen transmission",
                        "kilometer",
                        "RER",
                    ),
                ],
                "state": ["gaseous"],
                "distance": [
                    500,
                    # 1000
                ],
                "regional storage": (
                    "geological hydrogen storage",
                    "hydrogen storage",
                    "kilogram",
                    "RER",
                ),
                "lifetime": 40 * 400000 * 1e3,
            },
        }

        for region in self.regions:

            for act in [
                "hydrogen embrittlement inhibition",
                "geological hydrogen storage",
                "hydrogenation of hydrogen",
                "dehydrogenation of hydrogen",
                "Hydrogen refuelling station",
            ]:

                dataset = wt.copy_to_new_location(
                    ws.get_one(self.original_db, ws.equals("name", act)), region
                )

                for exc in ws.production(dataset):
                    if "input" in exc:
                        exc.pop("input")

                dataset = relink_technosphere_exchanges(
                    dataset, self.database, self.model
                )

                self.database.append(dataset)
                # Add created dataset to `self.list_datasets`
                self.list_datasets.append(
                    (dataset["name"], dataset["reference product"], dataset["location"])
                )

            for fuel, activities in fuel_activities.items():
                for act in activities:
                    for vehicle, config in supply_chain_scenarios.items():
                        for state in config["state"]:
                            for distance in config["distance"]:

                                # dataset creation
                                new_act = {
                                    "location": region,
                                    "name": f"hydrogen supply, {act[1]}, by {vehicle}, as {state}, over {distance} km",
                                    "reference product": "hydrogen, 700 bar",
                                    "unit": "kilogram",
                                    "database": self.database[1]["database"],
                                    "code": str(uuid.uuid4().hex),
                                    "comment": f"Dataset representing {fuel} supply, generated by `premise`.",
                                }

                                # production flow
                                new_exc = [
                                    {
                                        "uncertainty type": 0,
                                        "loc": 1,
                                        "amount": 1,
                                        "type": "production",
                                        "production volume": 1,
                                        "product": "hydrogen, 700 bar",
                                        "name": f"hydrogen supply, {act[1]}, by {vehicle}, as {state}, over {distance} km",
                                        "unit": "kilogram",
                                        "location": region,
                                    }
                                ]

                                # transport
                                for trspt_type in config["type"]:
                                    new_exc.append(
                                        {
                                            "uncertainty type": 0,
                                            "amount": distance / 1000
                                            if trspt_type[2] == "ton kilometer"
                                            else distance
                                            / 2
                                            * (1 / config["lifetime"]),
                                            "type": "technosphere",
                                            "product": trspt_type[1],
                                            "name": trspt_type[0],
                                            "unit": trspt_type[2],
                                            "location": trspt_type[3],
                                            "comment": f"Transport over {distance} km by {vehicle}.",
                                        }
                                    )

                                    string = (
                                        f"Transport over {distance} km by {vehicle}."
                                    )

                                    if "comment" in new_act:
                                        new_act["comment"] += string
                                    else:
                                        new_act["comment"] = string

                                # need for inhibitor and purification if CNG pipeline
                                # electricity for purification: 2.46 kWh/kg H2
                                if vehicle == "CNG pipeline":

                                    inhibbitor_ds = ws.get_one(
                                        self.database,
                                        ws.contains(
                                            "name", "hydrogen embrittlement inhibition"
                                        ),
                                        ws.equals("location", region),
                                    )

                                    new_exc.append(
                                        {
                                            "uncertainty type": 0,
                                            "amount": 1,
                                            "type": "technosphere",
                                            "product": inhibbitor_ds[
                                                "reference product"
                                            ],
                                            "name": inhibbitor_ds["name"],
                                            "unit": inhibbitor_ds["unit"],
                                            "location": region,
                                            "comment": "Injection of an inhibiting gas (oxygen) to prevent embritllement of metal.",
                                        }
                                    )

                                    string = (
                                        " 2.46 kWh/kg H2 is needed to purify the hydrogen from the inhibiting gas."
                                        " The recovery rate for hydrogen after separation from the inhibitor gas is 93%."
                                    )
                                    if "comment" in new_act:
                                        new_act["comment"] += string
                                    else:
                                        new_act["comment"] = string

                                if "regional storage" in config:

                                    storage_ds = ws.get_one(
                                        self.database,
                                        ws.contains(
                                            "name", "geological hydrogen storage"
                                        ),
                                        ws.equals("location", region),
                                    )

                                    new_exc.append(
                                        {
                                            "uncertainty type": 0,
                                            "amount": 1,
                                            "type": "technosphere",
                                            "product": storage_ds["reference product"],
                                            "name": storage_ds["name"],
                                            "unit": storage_ds["unit"],
                                            "location": region,
                                            "comment": "Geological storage (salt cavern).",
                                        }
                                    )

                                    string = " Geological storage is added. It includes 0.344 kWh for the injection and pumping of 1 kg of H2."
                                    if "comment" in new_act:
                                        new_act["comment"] += string
                                    else:
                                        new_act["comment"] = string

                                # electricity for compression
                                if state == "gaseous":

                                    # if transport by truck, compression from 25 bar to 500 bar for teh transport
                                    # and from 500 bar to 900 bar for dispensing in 700 bar storage tanks

                                    # if transport by pipeline, initial compression from 25 bar to 100 bar
                                    # and 0.6 kWh re-compression every 250 km
                                    # and finally from 100 bar to 900 bar for dispensing in 700 bar storage tanks

                                    if vehicle == "truck":
                                        electricity_comp = get_compression_effort(
                                            25, 500, 1000
                                        )
                                        electricity_comp += get_compression_effort(
                                            500, 900, 1000
                                        )
                                    else:
                                        electricity_comp = get_compression_effort(
                                            25, 100, 1000
                                        ) + (0.6 * distance / 250)
                                        electricity_comp += get_compression_effort(
                                            100, 900, 1000
                                        )

                                    new_exc.append(
                                        {
                                            "uncertainty type": 0,
                                            "amount": electricity_comp,
                                            "type": "technosphere",
                                            "product": "electricity, low voltage",
                                            "name": "market group for electricity, low voltage",
                                            "unit": "kilowatt hour",
                                            "location": "RoW",
                                        }
                                    )

                                    string = (
                                        f" {electricity_comp} kWh is added to compress from 25 bar 100 bar (if pipeline)"
                                        f"or 500 bar (if truck), and then to 900 bar to dispense in storage tanks at 700 bar. "
                                        " Additionally, if transported by pipeline, there is re-compression (0.6 kWh) every 250 km."
                                    )

                                    if "comment" in new_act:
                                        new_act["comment"] += string
                                    else:
                                        new_act["comment"] = string

                                # electricity for liquefaction
                                if state == "liquid":
                                    # liquefaction electricity need
                                    # currently, 12 kWh/kg H2
                                    # mid-term, 8 kWh/ kg H2
                                    # by 2050, 6 kWh/kg H2
                                    electricity_comp = np.clip(
                                        np.interp(
                                            self.year, [2020, 2035, 2050], [12, 8, 6]
                                        ),
                                        12,
                                        6,
                                    )
                                    new_exc.append(
                                        {
                                            "uncertainty type": 0,
                                            "amount": electricity_comp,
                                            "type": "technosphere",
                                            "product": "electricity, low voltage",
                                            "name": "market group for electricity, low voltage",
                                            "unit": "kilowatt hour",
                                            "location": "RoW",
                                        }
                                    )

                                    string = f" {electricity_comp} kWh is added to liquefy the hydrogen. "
                                    if "comment" in new_act:
                                        new_act["comment"] += string
                                    else:
                                        new_act["comment"] = string

                                # electricity for hydrogenation, dehydrogenation and compression at delivery
                                if state == "liquid organic compound":

                                    hydrogenation_ds = ws.get_one(
                                        self.database,
                                        ws.equals("name", "hydrogenation of hydrogen"),
                                        ws.equals("location", region),
                                    )

                                    dehydrogenation_ds = ws.get_one(
                                        self.database,
                                        ws.equals(
                                            "name", "dehydrogenation of hydrogen"
                                        ),
                                        ws.equals("location", region),
                                    )

                                    new_exc.extend(
                                        [
                                            {
                                                "uncertainty type": 0,
                                                "amount": 1,
                                                "type": "technosphere",
                                                "product": hydrogenation_ds[
                                                    "reference product"
                                                ],
                                                "name": hydrogenation_ds["name"],
                                                "unit": hydrogenation_ds["unit"],
                                                "location": region,
                                            },
                                            {
                                                "uncertainty type": 0,
                                                "amount": 1,
                                                "type": "technosphere",
                                                "product": dehydrogenation_ds[
                                                    "reference product"
                                                ],
                                                "name": dehydrogenation_ds["name"],
                                                "unit": dehydrogenation_ds["unit"],
                                                "location": region,
                                            },
                                        ]
                                    )

                                    # After dehydrogenation at ambient temperature at delivery
                                    # the hydrogen needs to be compressed up to 900 bar to be dispensed
                                    # in 700 bar storage tanks

                                    electricity_comp = get_compression_effort(
                                        25, 900, 1000
                                    )

                                    new_exc.append(
                                        {
                                            "uncertainty type": 0,
                                            "amount": electricity_comp,
                                            "type": "technosphere",
                                            "product": "electricity, low voltage",
                                            "name": "market group for electricity, low voltage",
                                            "unit": "kilowatt hour",
                                            "location": "RoW",
                                        }
                                    )

                                    string = (
                                        " Hydrogenation and dehydrogenation of hydrogen included. "
                                        "Compression at delivery after dehydrogenation also included."
                                    )
                                    if "comment" in new_act:
                                        new_act["comment"] += string
                                    else:
                                        new_act["comment"] = string

                                # fetch the H2 production activity
                                h2_ds = ws.get_one(
                                    self.database,
                                    ws.equals("name", act[0]),
                                    ws.equals("location", region),
                                )

                                # include losses along the way
                                new_exc.append(
                                    {
                                        "uncertainty type": 0,
                                        "amount": 1
                                        + losses[vehicle][state][0](distance),
                                        "type": "technosphere",
                                        "product": h2_ds["reference product"],
                                        "name": h2_ds["name"],
                                        "unit": h2_ds["unit"],
                                        "location": region,
                                    }
                                )

                                string = losses[vehicle][state][1]
                                if "comment" in new_act:
                                    new_act["comment"] += string
                                else:
                                    new_act["comment"] = string

                                # add fuelling station, including storage tank
                                ds_h2_station = ws.get_one(
                                    self.database,
                                    ws.equals("name", "Hydrogen refuelling station"),
                                    ws.equals("location", region),
                                )

                                new_exc.append(
                                    {
                                        "uncertainty type": 0,
                                        "amount": 1
                                        / (
                                            600 * 365 * 40
                                        ),  # 1 over lifetime: 40 years, 600 kg H2/day
                                        "type": "technosphere",
                                        "product": ds_h2_station["reference product"],
                                        "name": ds_h2_station["name"],
                                        "unit": ds_h2_station["unit"],
                                        "location": region,
                                    }
                                )

                                # finally, add pre-cooling
                                # pre-cooling is needed before filling vehicle tanks
                                # as the hydrogen is pumped, the ambient temperature
                                # vaporizes the gas, and because of the Thomson-Joule effect,
                                # the gas temperature increases.
                                # Hence, a refrigerant is needed to keep the H2 as low as
                                # -30 C during pumping.

                                # https://www.osti.gov/servlets/purl/1422579 gives us a formula
                                # to estimate pre-cooling electricity need
                                # it requires a capacity utilization for the fuelling station
                                # as well as an ambient temperature
                                # we will use a temp of 25 C
                                # and a capacity utilization going from 10 kg H2/day in 2020
                                # to 150 kg H2/day in 2050

                                t_amb = 25
                                cap_util = np.interp(self.year, [2020, 2050], [10, 150])
                                el_pre_cooling = get_pre_cooling_energy(
                                    self.year, t_amb, cap_util
                                )

                                new_exc.append(
                                    {
                                        "uncertainty type": 0,
                                        "amount": el_pre_cooling,
                                        "type": "technosphere",
                                        "product": "electricity, low voltage",
                                        "name": "market group for electricity, low voltage",
                                        "unit": "kilowatt hour",
                                        "location": "RoW",
                                    }
                                )

                                string = (
                                    f"Pre-cooling electricity is considered ({el_pre_cooling}), "
                                    f"assuming an ambiant temperature of {t_amb}C "
                                    f"and a capacity utilization for the fuel station of {cap_util} kg/day."
                                )
                                if "comment" in new_act:
                                    new_act["comment"] += string
                                else:
                                    new_act["comment"] = string

                                new_act["exchanges"] = new_exc

                                new_act = relink_technosphere_exchanges(
                                    new_act, self.database, self.model
                                )

                                self.database.append(new_act)

                                # Add created dataset to `self.list_datasets`
                                self.list_datasets.append(
                                    (
                                        new_act["name"],
                                        new_act["reference product"],
                                        new_act["location"],
                                    )
                                )

    def generate_biogas_activities(self):

        fuel_activities = {
            "methane, from biomass": [
                "production of 2 wt-% potassium",
                "biogas upgrading - sewage sludge",
                "Biomethane, gaseous",
            ],
            "methane, synthetic": [
                "methane, from electrochemical methanation, with carbon from atmospheric CO2 capture",
                "Methane, synthetic, gaseous, 5 bar, from electrochemical methanation, at fuelling station",
            ],
        }

        for region in self.regions:
            for fuel, activities in fuel_activities.items():
                for f in activities:
                    if fuel == "methane, synthetic":
                        for CO2_type in [
                            (
                                "carbon dioxide, captured from atmosphere, with waste heat, and grid electricity",
                                "carbon dioxide, captured from the atmosphere",
                                "waste heat",
                            ),
                            (
                                "carbon dioxide, captured from atmosphere, with heat pump heat, and grid electricity",
                                "carbon dioxide, captured from the atmosphere",
                                "heat pump heat",
                            ),
                        ]:
                            dataset = wt.copy_to_new_location(
                                ws.get_one(self.original_db, ws.contains("name", f)),
                                region,
                            )

                            for exc in ws.production(dataset):
                                if "input" in exc:
                                    exc.pop("input")

                            for exc in ws.technosphere(dataset):
                                if (
                                    "carbon dioxide, captured from atmosphere"
                                    in exc["name"]
                                ):
                                    exc["name"] = CO2_type[0]
                                    exc["product"] = CO2_type[1]
                                    exc["location"] = region

                                    dataset["name"] += "using " + CO2_type[2]

                                    for prod in ws.production(dataset):
                                        prod["name"] += "using " + CO2_type[2]

                                if (
                                    "methane, from electrochemical methanation"
                                    in exc["name"]
                                ):
                                    exc["name"] += "using " + CO2_type[2]

                                    dataset["name"] = dataset["name"].replace(
                                        "from electrochemical methanation",
                                        f"from electrochemical methanation (H2 from electrolysis, CO2 from DAC using {CO2_type[2]})",
                                    )

                                    for prod in ws.production(dataset):
                                        prod["name"] = prod["name"].replace(
                                            "from electrochemical methanation",
                                            f"from electrochemical methanation (H2 from electrolysis, CO2 from DAC using {CO2_type[2]})",
                                        )

                            dataset = relink_technosphere_exchanges(
                                dataset, self.database, self.model
                            )

                            self.database.append(dataset)

                            # Add created dataset to `self.list_datasets`
                            self.list_datasets.append(
                                (
                                    dataset["name"],
                                    dataset["reference product"],
                                    dataset["location"],
                                )
                            )

                    else:

                        dataset = wt.copy_to_new_location(
                            ws.get_one(self.original_db, ws.contains("name", f)), region
                        )

                        for exc in ws.production(dataset):
                            if "input" in exc:
                                exc.pop("input")

                        dataset = relink_technosphere_exchanges(
                            dataset, self.database, self.model
                        )

                        self.database.append(dataset)

    def generate_synthetic_fuel_activities(self):

        fuel_activities = {
            "methanol": ["methanol", "hydrogen from electrolysis", "energy allocation"],
            "methanol, from coal": [
                "methanol",
                "hydrogen from coal gasification",
                "energy allocation",
            ],
            "fischer-tropsch": [
                "Fischer Tropsch process",
                "hydrogen from electrolysis",
                "energy allocation",
            ],
            "fischer-tropsch, from woody biomass": [
                "Fischer Tropsch process",
                "hydrogen from wood gasification",
                "energy allocation",
            ],
            "fischer-tropsch, from coal": [
                "Fischer Tropsch process",
                "hydrogen from coal gasification",
                "energy allocation",
            ],
        }

        for region in self.regions:
            for fuel, activities in fuel_activities.items():

                filter_ds = ws.get_many(
                    self.original_db, *[ws.contains("name", n) for n in activities],
                )

                for dataset in filter_ds:

                    ds_copy = wt.copy_to_new_location(dataset, region)

                    for exc in ws.production(ds_copy):
                        if "input" in exc:
                            exc.pop("input")

                    for exc in ws.technosphere(ds_copy):
                        if "carbon dioxide, captured from atmosphere" in exc["name"]:
                            exc[
                                "name"
                            ] = "carbon dioxide, captured from atmosphere, with heat pump heat, and grid electricity"
                            exc[
                                "product"
                            ] = "carbon dioxide, captured from the atmosphere"
                            exc["location"] = region

                    ds_copy = relink_technosphere_exchanges(
                        ds_copy, self.database, self.model
                    )

                    self.database.append(ds_copy)

                    # Add created dataset to `self.list_datasets`
                    self.list_datasets.append(
                        (
                            ds_copy["name"],
                            ds_copy["reference product"],
                            ds_copy["location"],
                        )
                    )

    def generate_biofuel_activities(self):
        """
        Create region-specific biofuel datasets.
        Update the conversion efficiency.
        :return:
        """

        # region -> climate dictionary
        d_region_climate = get_region_climate_mapping()
        # climate --> {crop type --> crop} dictionary
        d_climate_crop_type = get_crop_climate_mapping()

        added_acts = []

        regions = (r for r in self.regions if r != "World")
        for region in regions:
            climate_type = d_region_climate[region]

            for crop_type in d_climate_crop_type[climate_type]:
                crop = d_climate_crop_type[climate_type][crop_type]

                for original_ds in ws.get_many(
                    self.original_db,
                    ws.contains("name", crop),
                    ws.either(
                        *[
                            ws.contains("name", "supply of"),
                            ws.contains(
                                "name",
                                "via fermentation"
                                if crop_type != "oil"
                                else "via transesterification",
                            ),
                        ]
                    ),
                ):

                    dataset = wt.copy_to_new_location(original_ds, region)

                    for exc in ws.production(dataset):
                        if "input" in exc:
                            exc.pop("input")

                    dataset = relink_technosphere_exchanges(
                        dataset, self.database, self.model
                    )

                    # give it a production volume
                    for label in self.fuel_labels:
                        if crop_type in label:

                            dataset["production volume"] = (
                                self.iam_data.production_volumes.sel(
                                    region=region, variables=label
                                )
                                .interp(year=self.year)
                                .values.item(0)
                            )

                    # if this is a fuel conversion process
                    # we want to update the conversion efficiency
                    if any(
                        x in dataset["name"]
                        for x in ["fermentation", "transesterification"]
                    ) and any(
                        x in dataset["name"]
                        for x in ["Ethanol production", "Biodiesel production"]
                    ):
                        # modify efficiency
                        # fetch the `progress factor`, compared to 2020

                        vars = [
                            v
                            for v in self.iam_data.fuel_markets.variables.values
                            if crop_type.lower() in v.lower()
                            and any(
                                x.lower() in v.lower()
                                for x in ["bioethanol", "biodiesel"]
                            )
                        ]

                        if len(vars) > 0:
                            scaling_factor = 1 / self.find_iam_efficiency_change(
                                variable=vars, location=region,
                            )

                            # Rescale all the technosphere exchanges according to the IAM efficiency values

                            wurst.change_exchanges_by_constant_factor(
                                dataset,
                                scaling_factor,
                                [],
                                [ws.doesnt_contain_any("name", self.emissions_map)],
                            )

                            string = (
                                f"The process conversion efficiency has been rescaled by `premise` by {int((scaling_factor - 1) * 100)}%.\n"
                                f"To be in line with the scenario {self.scenario} of {self.model.upper()} "
                                f"in {self.year} in the region {region}.\n"
                            )

                            if "comment" in dataset:
                                dataset["comment"] += string
                            else:
                                dataset["comment"] = string

                            if "ethanol" in dataset["name"].lower():
                                dataset[
                                    "comment"
                                ] += "Bioethanol has a combustion CO2 emission factor of 1.91 kg CO2/kg."
                            if "biodiesel" in dataset["name"].lower():
                                dataset[
                                    "comment"
                                ] += "Biodiesel has a combustion CO2 emission factor of 2.85 kg CO2/kg."

                    # if this is a farming activity
                    # and if the product (crop) is not a residue
                    # and if we have land use info from the IAM
                    if (
                        "farming and supply" in dataset["name"].lower()
                        and crop_type.lower() in self.land_use_per_crop_type
                    ):

                        for exc in dataset["exchanges"]:
                            # we adjust the land use
                            if exc["type"] == "biosphere" and exc["name"].startswith(
                                "Occupation"
                            ):

                                # lower heating value, as received
                                lhv_ar = dataset["LHV [MJ/kg dry]"] * (
                                        1 - dataset["Moisture content [% wt]"]
                                )

                                # Ha/GJ
                                land_use = (
                                    self.iam_data.land_use.sel(
                                        region=region, variables=crop_type
                                    )
                                    .interp(year=self.year)
                                    .values
                                )
                                # HA to m2
                                land_use *= 10000
                                # m2/GJ to m2/MJ
                                land_use /= 1000

                                # m2/kg, as received
                                land_use *= lhv_ar

                                # update exchange value
                                exc["amount"] = land_use

                                string = (
                                    f"The land area occupied has been modified to {land_use}, "
                                    f"to be in line with the scenario {self.scenario} of {self.model.upper()} "
                                    f"in {self.year} in the region {region}. "
                                )
                                if "comment" in dataset:
                                    dataset["comment"] += string
                                else:
                                    dataset["comment"] = string

                    # if this is a farming activity
                    # and if the product (crop) is not a residue
                    # and if we have land use change CO2 info from the IAM
                    if (
                        "farming and supply" in dataset["name"].lower()
                        and crop_type.lower() in self.land_use_change_CO2_per_crop_type
                    ):

                        # then, we should include the Land Use Change-induced CO2 emissions
                        # those are given in kg CO2-eq./GJ of primary crop energy

                        # kg CO2/GJ
                        land_use_co2 = (
                            self.iam_data.land_use_change.sel(
                                region=region, variables=crop_type
                            )
                            .interp(year=self.year)
                            .values
                        )

                        # lower heating value, as received
                        lhv_ar = dataset["LHV [MJ/kg dry]"] * (
                            1 - dataset["Moisture content [% wt]"]
                        )

                        # kg CO2/MJ
                        land_use_co2 /= 1000
                        land_use_co2 *= lhv_ar

                        land_use_co2_exc = {
                            "uncertainty type": 0,
                            "loc": land_use_co2,
                            "amount": land_use_co2,
                            "type": "biosphere",
                            "name": "Carbon dioxide, from soil or biomass stock",
                            "unit": "kilogram",
                            "input": (
                                "biosphere3",
                                "78eb1859-abd9-44c6-9ce3-f3b5b33d619c",
                            ),
                            "categories": ("air", "non-urban air or from high stacks",),
                        }
                        dataset["exchanges"].append(land_use_co2_exc)

                        string = (
                            f"{land_use_co2} kg of land use-induced CO2 has been added by premise, "
                            f"to be in line with the scenario {self.scenario} of {self.model.upper()} "
                            f"in {self.year} in the region {region}."
                        )

                        if "comment" in dataset:
                            dataset["comment"] += string
                        else:
                            dataset["comment"] = string

                    if (dataset["name"], dataset["location"]) not in added_acts:
                        added_acts.append((dataset["name"], dataset["location"]))
                        self.database.append(dataset)

                        # Add created dataset to `self.list_datasets`
                        self.list_datasets.append(
                            (
                                dataset["name"],
                                dataset["reference product"],
                                dataset["location"],
                            )
                        )

    def get_fuel_mapping(self):
        """
        Define filter functions that decide which wurst datasets to modify.
        :return: dictionary that contains filters and functions
        :rtype: dict
        """

        return {
            fuel: {
                "find_share": self.fetch_fuel_share,
                "fuel filters": self.fuel_map[fuel],
            }
            for fuel in self.iam_data.fuel_markets.variables.values
        }

    def fetch_fuel_share(self, fuel, fuel_types, region):
        """ Return a fuel mix for a specific IAM region, for a specific year. """

        variables = [
            v
            for v in self.iam_data.fuel_markets.variables.values
            if any(x.lower() in v.lower() for x in fuel_types)
        ]

        return (
            self.iam_data.fuel_markets.sel(region=region, variables=fuel)
            / self.iam_data.fuel_markets.sel(region=region, variables=variables).sum(
                dim="variables"
            )
        ).values

    def relink_activities_to_new_markets(self):
        """
        Links fuel input exchanges to new datasets with the appropriate IAM location.

        Does not return anything.
        """

        # Filter all activities that consume fuels
        acts_to_ignore = set(x[0] for x in self.new_fuel_markets)

        for dataset in ws.get_many(
            self.database,
            ws.exclude(ws.either(*[ws.contains("name", n) for n in acts_to_ignore])),
        ):

            # check that a fuel input exchange is present in the list of inputs
            if any(
                f[0] == exc["name"]
                for exc in dataset["exchanges"]
                for f in self.new_fuel_markets
            ):
                amount_fossil_co2, amount_non_fossil_co2 = [0, 0]

                for name in [
                    ("market for petrol, unleaded", "petrol, unleaded", "kilogram"),
                    ("market for petrol, low-sulfur", "petrol, low-sulfur", "kilogram"),
                    ("market for diesel, low-sulfur", "diesel, low-sulfur", "kilogram"),
                    ("market for diesel", "diesel", "kilogram"),
                    (
                        "market for natural gas, high pressure",
                        "natural gas, high pressure",
                        "cubic meter",
                    ),
                    ("market for hydrogen, gaseous", "hydrogen, gaseous", "kilogram"),
                ]:

                    # checking that it is one of the markets
                    # that has been newly created
                    if name[0] in acts_to_ignore:

                        excs = list(
                            ws.get_many(
                                dataset["exchanges"],
                                ws.equals("name", name[0]),
                                ws.either(
                                    *[
                                        ws.equals("unit", "kilogram"),
                                        ws.equals("unit", "cubic meter"),
                                    ]
                                ),
                                ws.equals("type", "technosphere"),
                            )
                        )

                        amount = 0
                        for exc in excs:
                            amount += exc["amount"]
                            dataset["exchanges"].remove(exc)

                        if amount > 0:
                            if dataset["location"] in self.regions:
                                supplier_loc = dataset["location"]

                            else:
                                new_loc = self.geo.ecoinvent_to_iam_location(
                                    dataset["location"]
                                )
                                supplier_loc = (
                                    new_loc
                                    if new_loc in self.regions
                                    else self.regions[0]
                                )

                            new_exc = {
                                "name": name[0],
                                "product": name[1],
                                "amount": amount,
                                "type": "technosphere",
                                "unit": name[2],
                                "location": supplier_loc,
                            }

                            dataset["exchanges"].append(new_exc)

                            amount_fossil_co2 += (
                                amount
                                * self.new_fuel_markets[(name[0], supplier_loc)][
                                    "fossil CO2"
                                ]
                            )
                            amount_non_fossil_co2 += (
                                amount
                                * self.new_fuel_markets[(name[0], supplier_loc)][
                                    "non-fossil CO2"
                                ]
                            )

                # update fossil and biogenic CO2 emissions
                list_items_to_ignore = [
                    "blending",
                    "market group",
                    "lubricating oil production",
                    "petrol production",
                ]
                if amount_non_fossil_co2 > 0 and not any(
                    x in dataset["name"].lower() for x in list_items_to_ignore
                ):

                    # test for the presence of a fossil CO2 flow
                    if not [
                        e
                        for e in dataset["exchanges"]
                        if "Carbon dioxide, fossil" in e["name"]
                    ]:
                        print(
                            f"Warning: {dataset['name'], dataset['location']} has no fossil CO2 output flow."
                        )

                    # subtract the biogenic CO2 amount to the
                    # initial fossil CO2 emission amount

                    for exc in ws.biosphere(
                        dataset, ws.equals("name", "Carbon dioxide, fossil")
                    ):
                        if (exc["amount"] - amount_non_fossil_co2) < 0:
                            exc["amount"] = 0
                        else:
                            exc["amount"] -= amount_non_fossil_co2

                    # add the biogenic CO2 emission flow
                    non_fossil_co2 = {
                        "uncertainty type": 0,
                        "loc": amount_non_fossil_co2,
                        "amount": amount_non_fossil_co2,
                        "type": "biosphere",
                        "name": "Carbon dioxide, from soil or biomass stock",
                        "unit": "kilogram",
                        "categories": ("air",),
                        "input": ("biosphere3", "e4e9febc-07c1-403d-8d3a-6707bb4d96e6"),
                    }

                    dataset["exchanges"].append(non_fossil_co2)

    def select_multiple_suppliers(self, fuel, d_fuels, dataset, look_for):

        # We have several potential fuel suppliers
        # We will look up their respective production volumes
        # And include them proportionally to it

        ecoinvent_regions = self.geo.iam_to_ecoinvent_location(dataset["location"])
        possible_locations = [
            dataset["location"],
            *ecoinvent_regions,
            "RER",
            "Europe without Switzerland",
            "RoW",
            "GLO",
        ]
        possible_names = list(d_fuels[fuel]["fuel filters"])
        suppliers, counter = [], 0

        while not suppliers:
            suppliers = list(
                ws.get_many(
                    self.database,
                    ws.either(*[ws.contains("name", sup) for sup in possible_names]),
                    ws.equals("location", possible_locations[counter]),
                    ws.either(
                        *[ws.contains("reference product", item) for item in look_for]
                    ),
                    ws.doesnt_contain_any(
                        "reference product", ["petroleum coke", "petroleum gas", "wax"]
                    ),
                )
            )
            counter += 1

            if "low-sulfur" in dataset["name"]:
                suppliers = filter_results(
                    item_to_look_for="unleaded",
                    results=suppliers,
                    field_to_look_at="reference product")

            if "unleaded" in dataset["name"]:
                suppliers = filter_results(
                    item_to_look_for="low-sulfur",
                    results=suppliers,
                    field_to_look_at="reference product",
                )

        suppliers = get_shares_from_production_volume(suppliers)

        return suppliers

    def generate_fuel_supply_chains(self):
        """ Duplicate fuel chains and make them IAM region-specific """

        # DAC datasets
        print("Generate region-specific direct air capture processes.")
        self.generate_dac_activities()

        # hydrogen
        print("Generate region-specific hydrogen production pathways.")
        self.generate_hydrogen_activities()

        # biogas
        print("Generate region-specific biogas and syngas supply chains.")
        self.generate_biogas_activities()

        # synthetic fuels
        print("Generate region-specific synthetic fuel supply chains.")
        self.generate_synthetic_fuel_activities()

        # biofuels
        print("Generate region-specific biofuel fuel supply chains.")
        self.generate_biofuel_activities()

    def generate_fuel_markets(self):
        """ Create new fuel supply chains
        and update existing fuel markets """

        # Create new fuel supply chains
        self.generate_fuel_supply_chains()

        print("Generate new fuel markets.")

        fuel_markets = [
            ("market for petrol, unleaded", "petrol, unleaded", "kilogram", 42.6),
            ("market for petrol, low-sulfur", "petrol, low-sulfur", "kilogram", 42.6),
            ("market for diesel, low-sulfur", "diesel, low-sulfur", "kilogram", 43),
            ("market for diesel", "diesel", "kilogram", 43),
            (
                "market for natural gas, high pressure",
                "natural gas, high pressure",
                "cubic meter",
                47.5,
            ),
            ("market for hydrogen, gaseous", "hydrogen, gaseous", "kilogram", 120),
        ]

        # refresh the fuel filters
        # as some have been created in the meanwhile
        mapping = InventorySet(self.database)
        self.fuel_map = mapping.generate_fuel_map()
        d_fuels = self.get_fuel_mapping()

        # to log new fuel markets
        new_fuel_markets = [
            [
                "market name",
                "location",
                "unit",
                "reference product",
                "fuel type",
                "supplier name",
                "supplier reference product",
                "supplier location",
                "supplier unit",
                "fuel mix share (energy-wise)",
                "amount supplied [kg]",
                "LHV [mj/kg]",
                "CO2 emmission factor [kg CO2]",
                "biogenic share",
            ]
        ]

        for fuel_market in fuel_markets:

            print(f"--> {fuel_market[0]}")

            if any(
                fuel_market[1].split(", ", maxsplit=1)[0] in f for f in self.fuel_labels
            ):

                d_act = self.fetch_proxies(
                    name=fuel_market[0],
                    ref_prod=fuel_market[1],
                    production_variable=self.iam_data.fuel_markets.variables.values,
                    relink=True,
                )

                for region, dataset in d_act.items():

                    string = " Fuel market composition: "
                    fossil_co2, non_fossil_co2, final_lhv = [0, 0, 0]

                    # remove existing fuel providers
                    dataset["exchanges"] = [
                        exc
                        for exc in dataset["exchanges"]
                        if exc["type"] != "technosphere"
                        or (
                            exc["product"] != dataset["reference product"]
                            and not any(
                                x in exc["name"]
                                for x in ["production", "evaporation", "import"]
                            )
                        )
                    ]

                    if "petrol" in fuel_market[0]:
                        look_for = ["petrol", "ethanol", "methanol", "gasoline"]

                    elif "diesel" in fuel_market[0]:
                        look_for = ["diesel", "biodiesel"]

                    elif "natural gas" in fuel_market[0]:
                        look_for = ["natural gas", "biomethane"]

                    else:
                        look_for = ["hydrogen"]

                    fuels = (f for f in d_fuels if any(x in f for x in look_for))

                    for fuel in fuels:
                        if fuel in self.fuel_labels:

                            share = d_fuels[fuel]["find_share"](fuel, look_for, region)

                            if share > 0:
                                possible_suppliers = self.select_multiple_suppliers(
                                    fuel, d_fuels, dataset, look_for
                                )

                                if not possible_suppliers:
                                    print(
                                        f"ISSUE with {fuel} in {region} for ds in location {dataset['location']}"
                                    )

                                    print(d_fuels[fuel])

                                for key, val in possible_suppliers.items():
                                    # m3 to kg conversion
                                    if key[-1] != fuel_market[2]:
                                        conversion_factor = 0.679
                                    else:
                                        # kg to kg
                                        conversion_factor = 1

                                    supplier_share = share * val

                                    # LHV of the fuel before update
                                    reference_lhv = fuel_market[3]

                                    # amount of fuel input
                                    # corrected by the LHV of the initial fuel
                                    # so that the overall composition maintains
                                    # the same average LHV

                                    amount = (
                                        supplier_share
                                        * (reference_lhv / self.fuels_lhv[fuel])
                                        * conversion_factor
                                    )

                                    fossil_co2 += (
                                        amount
                                        * self.fuels_lhv[fuel]
                                        * self.fuel_co2[fuel]["co2"]
                                        * (1 - self.fuel_co2[fuel]["bio_share"])
                                    )
                                    non_fossil_co2 += (
                                        amount
                                        * self.fuels_lhv[fuel]
                                        * self.fuel_co2[fuel]["co2"]
                                        * self.fuel_co2[fuel]["bio_share"]
                                    )

                                    final_lhv += amount * self.fuels_lhv[fuel]

                                    dataset["exchanges"].append(
                                        {
                                            "uncertainty type": 0,
                                            "amount": amount,
                                            "product": key[2],
                                            "name": key[0],
                                            "unit": key[-1],
                                            "location": key[1],
                                            "type": "technosphere",
                                        }
                                    )

                                    # log
                                    new_fuel_markets.append(
                                        [
                                            dataset["name"],
                                            dataset["location"],
                                            dataset["unit"],
                                            dataset["reference product"],
                                            fuel,
                                            key[0],
                                            key[2],
                                            key[1],
                                            key[-1],
                                            share,
                                            amount,
                                            self.fuels_lhv[fuel],
                                            self.fuel_co2[fuel]["co2"],
                                            self.fuel_co2[fuel]["bio_share"],
                                        ]
                                    )

                                string += f"{fuel.capitalize()}: {(share * 100):.1f} pct @ {self.fuels_lhv[fuel]} MJ/kg. "

                    string += f"Final average LHV of {final_lhv} MJ/kg."

                    if "comment" in dataset:
                        dataset["comment"] += string
                    else:
                        dataset["comment"] = string

                    # add two new fields: `fossil CO2` and `biogenic CO2`
                    dataset["fossil CO2"] = fossil_co2
                    dataset["non-fossil CO2"] = non_fossil_co2
                    dataset["LHV"] = final_lhv

                    # add fuel market to the dictionary
                    self.new_fuel_markets[(dataset["name"], dataset["location"])] = {
                        "fossil CO2": fossil_co2,
                        "non-fossil CO2": non_fossil_co2,
                        "LHV": final_lhv,
                    }

                self.database.extend(d_act.values())

        # write log to CSV
        with open(
            DATA_DIR
            / f"logs/log created fuel markets {self.model} {self.scenario} {self.year}-{date.today()}.csv",
            "a",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for line in new_fuel_markets:
                writer.writerow(line)

        self.relink_activities_to_new_markets()

        print(f"Log of deleted fuel markets saved in {DATA_DIR / 'logs'}")
        print(f"Log of created fuel markets saved in {DATA_DIR / 'logs'}")

        return self.database
