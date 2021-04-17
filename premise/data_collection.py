from . import DATA_DIR
import pandas as pd
from pathlib import Path
import csv
from cryptography.fernet import Fernet
from io import StringIO


IAM_ELEC_MARKETS = DATA_DIR / "electricity" / "electricity_markets.csv"
IAM_ELEC_EFFICIENCIES = DATA_DIR / "electricity" / "electricity_efficiencies.csv"
IAM_ELEC_EMISSIONS = DATA_DIR / "electricity" / "electricity_emissions.csv"
GAINS_TO_IAM_FILEPATH = DATA_DIR / "GAINS_emission_factors" / "GAINStoREMINDtechmap.csv"
GNR_DATA = DATA_DIR / "cement" / "additional_data_GNR.csv"


class IAMDataCollection:
    """
    Class that extracts data from IAM output files.

    :ivar pathway: name of a IAM pathway
    :vartype pathway: str

    """

    def __init__(self, model, pathway, year, filepath_iam_files, key):
        self.model = model
        self.pathway = pathway
        self.year = year
        self.filepath_iam_files = filepath_iam_files
        self.key = key
        self.data = self.get_iam_data()
        self.regions = [r for r in self.data.region.values]

        self.gains_data = self.get_gains_data()
        self.gnr_data = self.get_gnr_data()
        self.electricity_market_labels = self.get_iam_electricity_market_labels()
        self.electricity_efficiency_labels = (
            self.get_iam_electricity_efficiency_labels()
        )
        self.electricity_emission_labels = self.get_iam_electricity_emission_labels()
        self.rev_electricity_market_labels = self.get_rev_electricity_market_labels()
        self.rev_electricity_efficiency_labels = (
            self.get_rev_electricity_efficiency_labels()
        )
        self.electricity_markets = self.get_iam_electricity_markets()
        self.electricity_efficiencies = self.get_iam_electricity_efficiencies()
        self.electricity_emissions = self.get_gains_electricity_emissions()
        self.cement_emissions = self.get_gains_cement_emissions()
        self.steel_emissions = self.get_gains_steel_emissions()


    def get_iam_electricity_emission_labels(self):
        """
        Loads a csv file into a dictionary. This dictionary contains labels of electricity emissions
        in the IAM selected.

        :return: dictionary that contains emission names equivalence
        :rtype: dict
        """
        d = dict()
        with open(IAM_ELEC_EMISSIONS) as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                if row[0] == self.model:
                    d[row[1]] = row[2]
        return d

    def get_iam_electricity_market_labels(self):
        """
        Loads a csv file into a dictionary. This dictionary contains labels of electricity markets
        in the IAM.

        :return: dictionary that contains market names equivalence
        :rtype: dict
        """

        d = dict()
        with open(IAM_ELEC_MARKETS) as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                if row[0] == self.model:
                    d[row[1]] = row[2]
        return d

    def get_iam_electricity_efficiency_labels(self):
        """
        Loads a csv file into a dictionary. This dictionary contains labels of electricity technologies efficiency
        in the IAM.

        :return: dictionary that contains market names equivalence
        :rtype: dict
        """

        d = dict()
        with open(IAM_ELEC_EFFICIENCIES) as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                if row[0] == self.model:
                    d[row[1]] = row[2]
        return d

    def get_rev_electricity_market_labels(self):
        return {v: k for k, v in self.electricity_market_labels.items()}

    def get_rev_electricity_efficiency_labels(self):
        return {v: k for k, v in self.electricity_efficiency_labels.items()}

    def get_iam_data(self):
        """
        Read the IAM result file and return an `xarray` with dimensions:
        * region
        * variable
        * year

        :return: an multi-dimensional array with IAM data
        :rtype: xarray.core.dataarray.DataArray

        """

        file_ext = self.model + "_" + self.pathway + ".csv"
        filepath = Path(self.filepath_iam_files) / file_ext


        if self.key is None:
            # Uses a non-encrypted file

            with open(filepath, "rb") as file:
                # read the encrypted data
                encrypted_data = file.read()
            # create a temp csv-like file to pass to pandas.read_csv()
            DATA = StringIO(str(encrypted_data, 'latin-1'))

        else:
            # Uses an encrypted file

            f = Fernet(self.key)
            with open(filepath, "rb") as file:
                # read the encrypted data
                encrypted_data = file.read()

            # decrypt data
            decrypted_data = f.decrypt(encrypted_data)
            DATA = StringIO(str(decrypted_data, 'latin-1'))

        if self.model == "remind":
            df = pd.read_csv(
                DATA, sep=";", index_col=["Region", "Variable", "Unit"], encoding="latin-1"
            ).drop(columns=["Model", "Scenario"])

            # Filter the dataframe
            list_var = ("SE", "Tech", "FE", "Production", "Emi|CCO2", "Emi|CO2")

        elif self.model == "image":

            df = pd.read_csv(DATA, index_col=[2, 3, 4],
                             encoding="latin-1",
                             sep=";").drop(
                columns=["Model", "Scenario"]
            )

            # Filter the dataframe
            list_var = (
                "Secondary Energy",
                "Efficiency",
                "Final Energy",
                "Production",
                "Emissions"
            )
        else:
            raise ValueError("The IAM model name {} is not valid. Currently supported: 'remind' or 'image'".format(self.model))

        if len(df.columns == 20):
            df.drop(columns=df.columns[-1], inplace=True)

        df.columns = df.columns.astype(int)
        df = df.reset_index()

        df = df.loc[df["Variable"].str.startswith(list_var)]

        df = df.rename(
            columns={"Region": "region", "Variable": "variables", "Unit": "unit"}
        )

        array = (
            df.melt(
                id_vars=["region", "variables", "unit"],
                var_name="year",
                value_name="value",
            )[["region", "variables", "year", "value"]]
            .groupby(["region", "variables", "year"])["value"]
            .mean()
            .to_xarray()
        )

        return array

    @staticmethod
    def get_gains_data():
        """
        Read the GAINS emissions csv file and return an `xarray` with dimensions:
        * region
        * pollutant
        * sector
        * year

        :return: an multi-dimensional array with GAINS emissions data
        :rtype: xarray.core.dataarray.DataArray

        """
        filename = "GAINS emission factors.csv"
        filepath = DATA_DIR / "GAINS_emission_factors" / filename

        gains_emi = pd.read_csv(
            filepath,
            skiprows=4,
            names=["year", "region", "GAINS", "pollutant", "pathway", "factor"],
        )
        gains_emi["unit"] = "Mt/TWa"
        gains_emi = gains_emi[gains_emi.pathway == "SSP2"]

        sector_mapping = pd.read_csv(GAINS_TO_IAM_FILEPATH).drop(
            ["noef", "elasticity"], axis=1
        )

        gains_emi = (
            gains_emi.join(sector_mapping.set_index("GAINS"), on="GAINS")
            .dropna()
            .drop(["pathway", "REMIND"], axis=1)
            .pivot_table(
                index=["region", "GAINS", "pollutant", "unit"],
                values="factor",
                columns="year",
            )
        )

        gains_emi = gains_emi.reset_index()
        gains_emi = gains_emi.melt(
            id_vars=["region", "pollutant", "unit", "GAINS"],
            var_name="year",
            value_name="value",
        )[["region", "pollutant", "GAINS", "year", "value"]]
        gains_emi = gains_emi.rename(columns={"GAINS": "sector"})
        array = (
            gains_emi.groupby(["region", "pollutant", "year", "sector"])["value"]
            .mean()
            .to_xarray()
        )

        return array / 8760  # per TWha --> per TWh

    def get_gnr_data(self):
        """
        Read the GNR csv file on cement production and return an `xarray` with dimensions:
        * region
        * year
        * variables

        :return: an multi-dimensional array with GNR data
        :rtype: xarray.core.dataarray.DataArray

        :return:
        """
        df = pd.read_csv(GNR_DATA)
        df = df[["region", "year", "variables", "value"]]

        gnr_array = (
            df.groupby(["region", "year", "variables"]).mean()["value"].to_xarray()
        )
        gnr_array = gnr_array.interpolate_na(
            dim="year", method="linear", fill_value="extrapolate"
        )
        gnr_array = gnr_array.interp(year=self.year)
        gnr_array = gnr_array.fillna(0)

        return gnr_array

    def get_iam_electricity_markets(self, drop_hydrogen=True):
        """
        This method retrieves the market share for each electricity-producing technology, for a specified year,
        for each region provided by the IAM.
        Electricity production from hydrogen can be removed from the mix (unless specified, it is removed).

        :param drop_hydrogen: removes hydrogen from the region-specific electricity mix if `True`.
        :type drop_hydrogen: bool
        :return: an multi-dimensional array with electricity technologies market share for a given year, for all regions.
        :rtype: xarray.core.dataarray.DataArray

        """
        # If hydrogen is not to be considered, it is removed from the technologies labels list
        if drop_hydrogen:
            list_technologies = [
                l
                for l in list(self.electricity_market_labels.values())
                if "Hydrogen" not in l
            ]
        else:
            list_technologies = list(self.electricity_market_labels.values())

        # If the year specified is not contained within the range of years given by the IAM
        if (
            self.year < self.data.year.values.min()
            or self.year > self.data.year.values.max()
        ):
            raise KeyError("year not valid, must be between 2005 and 2100")

        # Finally, if the specified year falls in between two periods provided by the IAM
        else:
            # Interpolation between two periods
            data_to_return = self.data.loc[
                :, list_technologies, :
            ] / self.data.loc[:, list_technologies, :].groupby("region").sum(
                dim="variables"
            )
            return data_to_return

    def get_iam_electricity_efficiencies(self, drop_hydrogen=True):
        """
        This method retrieves efficiency values for electricity-producing technology, for a specified year,
        for each region provided by the IAM.
        Electricity production from hydrogen can be removed from the mix (unless specified, it is removed).

        :param drop_hydrogen: removes hydrogen from the region-specific electricity mix if `True`.
        :type drop_hydrogen: bool
        :return: an multi-dimensional array with electricity technologies market share for a given year, for all regions.
        :rtype: xarray.core.dataarray.DataArray

        """
        # If hydrogen is not to be considered, it is removed from the technologies labels list
        if drop_hydrogen:
            list_technologies = [
                l
                for l in list(self.electricity_efficiency_labels.values())
                if "Hydrogen" not in l
            ]
        else:
            list_technologies = list(self.electricity_efficiency_labels.values())

        # If the year specified is not contained within the range of years given by the IAM
        if (
            self.year < self.data.year.values.min()
            or self.year > self.data.year.values.max()
        ):
            raise KeyError("year not valid, must be between 2005 and 2100")

        # Finally, if the specified year falls in between two periods provided by the IAM
        else:
            # Interpolation between two periods
            data_to_interp_from = self.data.loc[:, list_technologies, :]

            if self.model == "remind":
                return (
                    data_to_interp_from.interp(year=self.year) / 100
                )  # Percentage to ratio

            if self.model == "image":
                return data_to_interp_from.interp(year=self.year)

    def get_gains_electricity_emissions(self):
        """
        This method retrieves emission values for electricity-producing technology, for a specified year,
        for each region provided by GAINS.

        :return: an multi-dimensional array with emissions for different technologies for a given year, for all regions.
        :rtype: xarray.core.dataarray.DataArray

        """
        # If the year specified is not contained within the range of years given by the IAM
        if (
            self.year < self.gains_data.year.values.min()
            or self.year > self.gains_data.year.values.max()
        ):
            raise KeyError("year not valid, must be between 2005 and 2100")

        # Finally, if the specified year falls in between two periods provided by the IAM
        else:
            # Interpolation between two periods
            return self.gains_data.sel(
                sector=[v for v in self.electricity_emission_labels.values()]
            ).interp(year=self.year)

    def get_gains_cement_emissions(self):
        """
        This method retrieves emission values for cement production, for a specified year,
        for each region provided by GAINS.

        :return: an multi-dimensional array with emissions for different technologies for a given year, for all regions.
        :rtype: xarray.core.dataarray.DataArray

        """
        # If the year specified is not contained within the range of years given by the IAM
        if (
            self.year < self.gains_data.year.values.min()
            or self.year > self.gains_data.year.values.max()
        ):
            raise KeyError("year not valid, must be between 2005 and 2100")

        # Finally, if the specified year falls in between two periods provided by the IAM
        else:
            # Interpolation between two periods
            return self.gains_data.sel(sector="CEMENT")

    def get_gains_steel_emissions(self):
        """
        This method retrieves emission values for steel production, for a specified year,
        for each region provided by GAINS.

        :return: an multi-dimensional array with emissions for different technologies for a given year, for all regions.
        :rtype: xarray.core.dataarray.DataArray

        """
        # If the year specified is not contained within the range of years given by the IAM
        if (
            self.year < self.gains_data.year.values.min()
            or self.year > self.gains_data.year.values.max()
        ):
            raise KeyError("year not valid, must be between 2005 and 2100")

        # Finally, if the specified year falls in between two periods provided by the IAM
        else:
            # Interpolation between two periods
            return self.gains_data.sel(sector="STEEL")
