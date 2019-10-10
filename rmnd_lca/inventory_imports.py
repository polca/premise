from . import DATA_DIR

import wurst
from wurst import searching as ws
from bw2io import ExcelImporter, Migration, SimaProCSVImporter
import csv

FILEPATH_BIOSPHERE_FLOWS = (DATA_DIR / "dict_biosphere.txt")


class BaseInventoryImport():
    """
    Base class for inventories that are to be merged with the ecoinvent database.

    :ivar new_database: the target database for the import
    :vartype new_database: NewDatabase
    :ivar path: Path to the imported inventory.
    :vartype path: Path
    """

    def __init__(self, new_database, path):
        self.db = new_database.db
        self.version = new_database.version
        self.biosphere_dict = self.get_biosphere_code()

        if not path.is_file():
            raise FileNotFoundError("The Carma inventory file could not be found.")
        self.import_db = self.load_inventory(path)

    def load_inventory(self, path):
        pass

    def prepare_inventory(self):
        pass

    def merge_inventory(self):
        self.prepare_inventory()
        self.db.extend(self.import_db)

    def get_biosphere_code(self):
        """
        Retrieve biosphere uuid for biosphere flows imported from Excel inventory.
        :return: dictionary with biosphere flow names as keys and uuid code as values
        :rtype: dict
        """

        if not FILEPATH_BIOSPHERE_FLOWS.is_file():
            raise FileNotFoundError(
                "The dictionary of biosphere flows could not be found."
            )

        csv_dict = {}

        with open(FILEPATH_BIOSPHERE_FLOWS) as f:
            input_dict = csv.reader(f, delimiter=";")
            for row in input_dict:
                csv_dict[(row[0], row[1], row[2], row[3])] = row[4]

        return csv_dict

    def add_product_field_to_exchanges(self):
        """Add the `product` key to the production exchange
        and format the category field for biosphere exchanges

        Modifies the imported DB in place.

        :param ext_db: Imported database to be merged with self.db
        :type ext_db: LCIImporter
        """
        for x in self.import_db.data:
            for y in x["exchanges"]:
                if y["type"] == "production" and "product" not in y:
                    y["product"] = x["reference product"]

        # Add a `product` field to technosphere exchanges
        for x in self.import_db.data:
            for y in x["exchanges"]:
                if y["type"] == "technosphere" and "product" not in y:

                    # Look first in the imported inventories
                    possibles = [
                        a["reference product"]
                        for a in self.import_db.data
                        if a["name"] == y["name"]
                        and a["location"] == y["location"]
                        and a["unit"] == y["unit"]
                    ]

                    # If not, look in the ecoinvent inventories
                    if len(possibles) == 0:
                        possibles = [
                            a["reference product"]
                            for a in self.db
                            if a["name"] == y["name"]
                            and a["location"] == y["location"]
                            and a["unit"] == y["unit"]
                        ]
                    if len(possibles) > 0:
                        y["product"] = possibles[0]
                    else:
                        raise IndexError(
                            'Some inventory exchanges in {} cannot be linked to the biosphere or the ecoinvent database.'.format(self.import_db.name))


class CarmaCCSInventory(BaseInventoryImport):
    def load_inventory(self, path):
        return ExcelImporter(path)

    def prepare_inventory(self):
        if(self.version == 3.6):
            # apply some updates to comply with ei 3.6
            new_technosphere_data = {
                'fields': ['name', 'reference product', 'location'],
                'data': [
                    (
                        ('market for water, decarbonised, at user', (), 'GLO'),
                        {
                            'name': ('market for water, decarbonised'),
                            'reference product': ('water, decarbonised'),
                            'location': ('DE'),
                        }
                    ),
                    (
                        ('market for water, completely softened, from decarbonised water, at user', (), 'GLO'),
                        {
                            'name': ('market for water, completely softened'),
                            'reference product': ('water, completely softened'),
                            'location': ('RER'),
                        }
                    ),
                    (
                        ('market for steam, in chemical industry', (), 'GLO'),
                        {
                            'location': ('RER'),
                            'reference product': ('steam, in chemical industry'),
                        }
                    ),
                    (
                        ('market for steam, in chemical industry', (), 'RER'),
                        {
                            'reference product': ('steam, in chemical industry'),
                        }
                    ),
                    (
                        ('zinc-lead mine operation', ('zinc concentrate',), 'GLO'),
                        {
                            'name': ('zinc mine operation'),
                            'reference product': ('bulk lead-zinc concentrate'),
                        }
                    ),
                    (
                        ('market for aluminium oxide', ('aluminium oxide',), 'GLO'),
                        {
                            'name': ('market for aluminium oxide, non-metallurgical'),
                            'reference product': ('aluminium oxide, non-metallurgical'),
                            'location': ('IAI Area, EU27 & EFTA'),
                        }
                    ),
                    (
                        ('platinum group metal mine operation, ore with high rhodium content', ('nickel, 99.5%',), 'ZA'),
                        {
                            'name': ('platinum group metal, extraction and refinery operations'),
                        }
                    )
                ]
            }

            Migration("migration_36").write(
                new_technosphere_data,
                description="Change technosphere names due to change from 3.5 to 3.6"
            )
            self.import_db.migrate("migration_36")

        # Fix biosphere exchanges
        for x in self.import_db.data:
            for y in x["exchanges"]:
                if y["type"] == "biosphere":
                    y["categories"] = tuple(y["categories"].split("::"))
                    if len(y["categories"]) > 1:
                        y["input"] = (
                            "biosphere3",
                            self.biosphere_dict[
                                (
                                    y["name"],
                                    y["categories"][0],
                                    y["categories"][1],
                                    y["unit"],
                                )
                            ],
                        )
                    else:
                        y["input"] = (
                            "biosphere3",
                            self.biosphere_dict[
                                (
                                    y["name"],
                                    y["categories"][0],
                                    "unspecified",
                                    y["unit"],
                                )
                            ],
                        )
        self.add_product_field_to_exchanges()

        # Add carbon storage for CCS technologies
        print("Add fossil carbon dioxide storage for CCS technologies.")
        self.add_negative_CO2_flows_for_biomass_CCS()

    def add_negative_CO2_flows_for_biomass_CCS(self):
        """
        Rescale the amount of all exchanges of carbon dioxide, non-fossil by a factor -9 (.9/-.1),
        to account for sequestered CO2.

        All CO2 capture and storage in the Carma datasets is assumed to be 90% efficient.
        Thus, we can simply find out what the new CO2 emission is and then we know how much gets stored in the ground.
        It's very important that we ONLY do this for biomass CCS plants, as only they will have negative emissions!

        Modifies in place (does not return anything).

        """
        for ds in ws.get_many(self.db, ws.contains('name', 'storage'), ws.equals('database', 'Carma CCS')):
            for exc in ws.biosphere(ds, ws.equals('name', 'Carbon dioxide, non-fossil')):
                wurst.rescale_exchange(exc, (0.9 / -0.1), remove_uncertainty=True)


# class BioenergyInventory(BaseInventoryImport):

#     def add_bioenergy_inventories(self):
#         """Add bioenergy datasets from the master thesis of Francesco Cozzolino (2018).

#         Modifies the database in place.
#         """
#         if not FILEPATH_BIO_INVENTORIES.is_file():
#             raise FileNotFoundError("The Bioenergy Inventory could not be found: {}".format(FILEPATH_BIO_INVENTORIES))

#         bio = SimaProCSVImporter(FILEPATH_BIO_INVENTORIES, name="biofuels attributional")

#         bio.migrate("simapro-ecoinvent-3.3")
#         bio.apply_strategies()

#         # Remove electricity datasets
#         bio.data = [a for a in bio.data if not a["name"].startswith("Electricity, ")]

#         for ds in bio.data:
#             for ex in ds["exchanges"]:
#                 if ex["name"] == 'Electricity, medium voltage':
#                     ex["name"] = 'market group for electricity, medium voltage'
#                     ex["location"] = 'RER'
        

#         migrations = {
#             'fields': ['name','reference product', 'location'],
#             'data': [
#                 (
#                     ('market for transport, freight, lorry >32 metric ton, EURO6', ('transport, freight, lorry >32 metric ton, EURO6',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                     ('market for transport, freight, inland waterways, barge', ('transport, freight, inland waterways, barge',),'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                 ('market for heat, in chemical industry', ('heat, in chemical industry',), 'RER' ),
#                     {
#                         'name': ('market for heat, from steam, in chemical industry'),
#                         'reference product': ('heat, from steam, in chemical industry')
#                     }
#                 ),
#                 (
#                     ('market for transport, pipeline, onshore, petroleum', ('transport, pipeline, onshore, petroleum',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                     ('market for ethanol, without water, in 99.7% solution state, from ethylene', ('ethanol, without water, in 99.7% solution state, from ethylene',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                     ('market for sulfuric acid', ('sulfuric acid',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                     ('market for transport, freight, lorry 7.5-16 metric ton, EURO6', ('transport, freight, lorry 7.5-16 metric ton, EURO6',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                     ('market for quicklime, milled, packed', ('quicklime, milled, packed',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                     ('market for transport, freight, inland waterways, barge', ('transport, freight, inland waterways, barge',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                     ('citric acid production', ('lime',), 'CN' ),
#                     {
#                         'reference product': ('citric acid'), # not needed in migration to cut-off db
#                     }
#                 ),
#                 (
#                     ('market for dolomite', ('dolomite',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#                 (
#                     ('market for calcium chloride', ('calcium chloride',), 'GLO' ),
#                     {
#                         'location': ('RER'),
#                     }
#                 ),
#             ]
#         }

#         Migration("ecoinvent_35").write(
#             migrations,
#             description="Change technosphere names due to change from 3.4 to 3.5"
#         )
#         bio.migrate("ecoinvent_35")

#         # Migrations for 3.6
#         if(self.destination.version == 3.6):
#             migrations = {
#                 'fields': ['name','reference product', 'location'],
#                 'data': [
#                     (
#                         ('market for transport, freight, sea, transoceanic tanker', ('transport, freight, sea, transoceanic tanker',), 'GLO' ),
#                         {
#                             'name': ('market for transport, freight, sea, tanker for liquid goods other than petroleum and liquefied natural gas'),
#                             'reference product': ('transport, freight, sea, tanker for liquid goods other than petroleum and liquefied natural gas'),
#                         }
#                     ),
#                     (
#                         ('market for water, decarbonised, at user', ('water, decarbonised, at user',), 'GLO'),
#                         {
#                             'name': ('market for water, decarbonised'),
#                             'reference product': ('water, decarbonised'),
#                             'location': ('DE'),
#                         }
#                     ),
#                     (
#                         ('market for water, completely softened, from decarbonised water, at user', ('water, completely softened, from decarbonised water, at user',), 'GLO'),
#                         {
#                             'name': ('market for water, completely softened'),
#                             'reference product': ('water, completely softened'),
#                             'location': ('RER'),
#                         }
#                     ),
#                     (
#                         ('market for concrete block', ('concrete block',), 'GLO'),
#                         {
#                             'location': ('DE'),
#                         }
#                     )
#                 ]
#             }

#             Migration("ecoinvent_36").write(
#                 migrations,
#                 description="Change technosphere names due to change from 3.5 to 3.6"
#             )
#             bio.migrate("ecoinvent_36")

#         # Add default locations
#         print("Set default locations on bioenergy inventory.")
#         wurst.default_global_location(bio.data)

#         self.add_product_field_to_exchanges(bio)
#         self.db.extend(bio)
