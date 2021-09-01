from . import DATA_DIR
import pandas as pd
from pathlib import Path
import csv
from cryptography.fernet import Fernet
from io import StringIO
import numpy as np
import xarray as xr


IAM_ELEC_MARKETS = DATA_DIR / "electricity" / "electricity_markets.csv"
IAM_FUELS_MARKETS = DATA_DIR / "fuels" / "fuel_labels.csv"
IAM_FUELS_EFFICIENCIES = DATA_DIR / "fuels" / "fuel_efficiencies.csv"
IAM_ELEC_EFFICIENCIES = DATA_DIR / "electricity" / "electricity_efficiencies.csv"
IAM_LIFETIMES = DATA_DIR / "lifetimes.csv"
IAM_ELEC_EMISSIONS = DATA_DIR / "electricity" / "electricity_emissions.csv"
GAINS_TO_IAM_FILEPATH = DATA_DIR / "GAINS_emission_factors" / "GAINStoREMINDtechmap.csv"
GNR_DATA = DATA_DIR / "cement" / "additional_data_GNR.csv"


class IAMDataCollection:
    """
    Class that extracts data from IAM output files.

    :ivar pathway: name of a IAM pathway
    :vartype pathway: str
    :ivar system_model: Can be `attributional` or `consequential`.
    :vartype pathway: str
    :ivar system_model: Time horizon (in years) to consider if `system_model` == `consequential`.
    :vartype pathway: int

    """

    def __init__(self, model, pathway, year, filepath_iam_files, key, system_model="attributionl", time_horizon=30):
        self.model = model
        self.pathway = pathway
        self.year = year
        self.filepath_iam_files = filepath_iam_files
        self.key = key
        self.data = self.get_iam_data()
        self.regions = [r for r in self.data.region.values]
        self.system_model = system_model
        self.time_horizon = time_horizon

        self.gains_data = self.get_gains_data()
        self.gnr_data = self.get_gnr_data()
        self.electricity_market_labels = self.get_iam_variable_labels(IAM_ELEC_MARKETS)
        self.electricity_efficiency_labels = self.get_iam_variable_labels(IAM_ELEC_EFFICIENCIES)
        self.electricity_emission_labels = self.get_iam_variable_labels(IAM_ELEC_EMISSIONS)

        self.electricity_markets = self.get_iam_electricity_markets()
        self.electricity_efficiencies = self.get_iam_electricity_efficiencies()
        self.electricity_emissions = self.get_gains_electricity_emissions()
        self.cement_emissions = self.get_gains_cement_emissions()
        self.steel_emissions = self.get_gains_steel_emissions()
        self.fuel_market_labels = self.get_iam_variable_labels(IAM_FUELS_MARKETS)
        self.fuel_efficiency_labels = self.get_iam_variable_labels(IAM_FUELS_EFFICIENCIES)
        self.fuel_markets = self.get_iam_fuel_markets()
        self.fuel_efficiencies = self.get_iam_fuel_efficiencies()

    def get_iam_variable_labels(self, filepath):
        """
        Loads a csv file into a dictionary.
        This dictionary contains common terminology to `premise`
        (fuel names, electricity production technologies, etc.) and its
        equivalent variable name in the IAM.

        :return: dictionary that contains fuel production names equivalence
        :rtype: dict
        """

        d = dict()
        with open(filepath) as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                if row[0] == self.model:
                    d[row[1]] = row[2]

        return d

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
            try:
                with open(filepath, "rb") as file:
                    # read the encrypted data
                    encrypted_data = file.read()
            except FileNotFoundError:
                file_ext = self.model + "_" + self.pathway + ".mif"
                filepath = Path(self.filepath_iam_files) / file_ext
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

            # if new sub-European regions a represent, we remove EUR and NEU
            if any(x in df.index.get_level_values("Region").unique() for x in ["ESC", "DEU", "NEN"]):
                df = df.loc[~df.index.get_level_values("Region").isin(["EUR", "NEU"])]

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
                "Emissions",
                "Land Use",
                "Emission Factor"
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
        gnr_array = gnr_array.interp(year=2020)
        gnr_array = gnr_array.fillna(0)

        return gnr_array

    def get_lifetime(self, list_tech):
        d = dict()
        with open(IAM_LIFETIMES) as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                d[row[0]] = row[1]

        arr = np.zeros_like(list_tech)

        for i, tech in enumerate(list_tech):
            lifetime = d[tech]
            arr[i] = lifetime

        return arr.astype(float)

    def transform_to_marginal_markets(self, data):

        shape = list(data.shape)
        shape[-1] = 1

        market_shares = xr.DataArray(
            np.zeros(tuple(shape)),
            dims=["region", "variables", "year"],
            coords={"region": data.coords["region"], "variables": data.variables, "year": [self.year]}
        )

        for region in data.coords["region"].values:


            current_shares = (
                data.sel(region=region, year=self.year) / data.sel(region=region, year=self.year).sum(dim="variables")
            )

            # we first need to calculate the average capital replacement rate of the market
            # which is here defined as the inverse of the production-weighted average lifetime
            lifetime = self.get_lifetime(current_shares.variables.values)

            avg_lifetime = np.sum(current_shares.values * lifetime)


            avg_cap_repl_rate = -1 / avg_lifetime

            volume_change = (data.sel(region=region).sum(dim="variables").interp(year=self.year + self.time_horizon) /
                data.sel(region=region).sum(dim="variables").interp(year=self.year)) - 1


            # first, we set CHP suppliers to zero
            # as electricity production is not a determining product for CHPs
            tech_to_ignore = [
                "CHP",
                "biomethane"
            ]
            data.loc[dict(variables=[v for v in data.variables.values
                                     if any(x in v for x in tech_to_ignore)], region=region)] = 0

            # second, we fetch the ratio between production in `self.year` and `self.year` + `time_horizon`
            # for each technology
            market_shares.loc[dict(region=region)] = (
                                                             data.sel(region=region).interp(
                                                                 year=self.year + self.time_horizon).values
                                                             / data.sel(region=region).interp(year=self.year).values
                                                     )[:, None] - 1



            market_shares.loc[dict(region=region)] = market_shares.loc[dict(region=region)].round(3)



            if region == "WEU":
                print(market_shares.loc[dict(region=region)])

            # we remove NaNs and np.inf
            market_shares.loc[dict(region=region)].values[market_shares.loc[dict(region=region)].values == np.inf] = 0
            market_shares.loc[dict(region=region)] = market_shares.loc[dict(region=region)].fillna(0)

            if region == "WEU":
                print(market_shares.loc[dict(region=region)])

            # we fetch the technologies' lifetimes
            lifetime = self.get_lifetime(market_shares.variables.values)
            # get the capital replacement rate
            # which is here defined as -1 / lifetime
            cap_repl_rate = -1 / lifetime

            if region == "WEU":
                print(cap_repl_rate)

            # subtract the capital replacement (which is negative) rate
            # to the changes market share
            market_shares.loc[dict(region=region, year=self.year)] += cap_repl_rate

            if region == "WEU":
                print(market_shares.loc[dict(region=region)])



            # market decreasing faster than the average capital renewal rate
            # in this case, the idea is that oldest/non-competitive technologies
            # are likely to supply by increasing their lifetime
            # as the market does not justify additional capacity installation
            if volume_change < avg_cap_repl_rate:

                print("decrease")

                # we remove suppliers with a positive growth
                market_shares.loc[dict(region=region)].values[market_shares.loc[dict(region=region)].values > 0] = 0
                # we reverse the sign of negative growth suppliers
                market_shares.loc[dict(region=region)] *= -1
                market_shares.loc[dict(region=region)] /= market_shares.loc[dict(region=region)].sum(dim="variables")

                # multiply by volumes at T0
                market_shares.loc[dict(region=region)] *= data.sel(region=region, year=self.year)
                market_shares.loc[dict(region=region)] /= market_shares.loc[dict(region=region)].sum(dim="variables")


            # increasing market or
            # market decreasing slowlier than the
            # capital renewal rate
            else:

                print("increase")

                # we remove suppliers with a negative growth
                market_shares.loc[dict(region=region)].values[market_shares.loc[dict(region=region)].values < 0] = 0
                market_shares.loc[dict(region=region)] /= market_shares.loc[dict(region=region)].sum(dim="variables")

                # multiply by volumes at T0
                market_shares.loc[dict(region=region)] *= data.sel(region=region, year=self.year)
                market_shares.loc[dict(region=region)] /= market_shares.loc[dict(region=region)].sum(dim="variables")

        return market_shares

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
            ]
            # give the array common labels
            list_vars = [var for var in list(self.electricity_market_labels.keys())
                         if var != "Hydrogen"] \
                if drop_hydrogen else list(self.electricity_market_labels.keys())

            data_to_return.coords["variables"] = list_vars

            if self.system_model == "consequential":

                data_to_return = self.transform_to_marginal_markets(data_to_return)

            else:
                data_to_return /= self.data.loc[:, list_technologies, :].groupby("region").sum(
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
            data = self.data.loc[:, list_technologies, :]

            data = (
                    data.interp(year=self.year)
                    / data.sel(year=2020)
            )

            # If we are looking at a year post 2020
            # and the ratio in efficiency change is inferior to 1
            # we correct it to 1, as we do not accept
            # that efficiency degrades over time
            if self.year > 2020:
                data.values[data.values < 1] = 1

            # Inversely, if we are looking at a year prior to 2020
            # and the ratio in efficiency change is superior to 1
            # we correct it to 1, as we do not accept
            # that efficiency in the past was higher than now
            if self.year < 2020:
                data.values[data.values > 1] = 1

            # convert NaNs to ones
            data = data.fillna(1)

            data.coords["variables"] = list(self.electricity_efficiency_labels.keys())

            return data

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

    def get_iam_fuel_markets(self):
        """
        This method retrieves the market share for each fuel-producing technology,
        for a specified year, for each region provided by the IAM.

        :return: an multi-dimensional array with electricity technologies market share for a given year, for all regions.
        :rtype: xarray.core.dataarray.DataArray

        """

        list_technologies = list(self.fuel_market_labels.values())

        # If the year specified is not contained within the range of years given by the IAM
        if (
            self.year < self.data.year.values.min()
            or self.year > self.data.year.values.max()
        ):
            raise KeyError("year not valid, must be between 2005 and 2100")

        # Finally, if the specified year falls in between two periods provided by the IAM
        else:

            # sometimes, the World region is either neglected
            # or wrongly evaluated
            # so we fix that here

            self.data.loc[dict(region="World", variables=list_technologies)] = self.data.loc[
                dict(
                    region=[r for r in self.data.coords["region"].values
                            if r != "World"],
                    variables=list_technologies
                )
            ].sum(dim="region")

            # Interpolation between two periods
            data_to_return = self.data.loc[
                :, list_technologies, :
            ]

            data_to_return.coords["variables"] = list(self.fuel_market_labels.keys())

            if self.system_model == "consequential":

                data_to_return = self.transform_to_marginal_markets(data_to_return)

            else:
                data_to_return = data_to_return.interp(year=self.year)
                data_to_return /= self.data.loc[:, list_technologies, :].interp(year=self.year).groupby("region").sum(
                dim="variables"
            )
            

            return data_to_return

    def get_iam_fuel_efficiencies(self):
        """
        This method retrieves the change in fuel production efficiency between the year in question and 2020,
        for each region provided by the IAM.
        If the efficiency drops after 2020, we ignore it and keep the change in efficiency ratio to 1.

        :return: an multi-dimensional array with electricity technologies market share for a given year, for all regions.
        :rtype: xarray.core.dataarray.DataArray

        """

        list_technologies = list(self.fuel_efficiency_labels.values())

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


            data = (
               data_to_interp_from.interp(year=self.year)
                / data_to_interp_from.sel(year=2020)
            )

            # If we are looking at a year post 2020
            # and the ratio in efficiency change is inferior to 1
            # we correct it to 1, as we do not accept
            # that efficiency degrades over time
            if self.year > 2020:
                data.values[data.values < 1] = 1

            # Inversely, if we are looking at a year prior to 2020
            # and the ratio in efficiency change is superior to 1
            # we correct it to 1, as we do not accept
            # that efficiency in the past was higher than now
            if self.year < 2020:
                data.values[data.values > 1] = 1

            data.coords["variables"] = list(self.fuel_efficiency_labels.keys())

            return data

