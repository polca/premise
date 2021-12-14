from .utils import *
import re
import numpy as np


class SolarPV:
    """
    Class that modifies solar PVs efficiency. It iterates through photovoltaic panel installation activities
    and modifies the area (m^2) required to achieve the given power capacity.
    For example, in ei 3.7, currently 22m^2 are required to achieve 3 kWp, meaning an efficiency of 12.6%,
    if we assume a maximal solar irradiation of 1000 W/m^2. In 2020, such installation should have an
    efficiency of about 18%.
    It is, for now, irrespective of the IAM pathway chosen.
    Source: p.357 of https://www.psi.ch/sites/default/files/import/ta/PublicationTab/Final-Report-BFE-Project.pdf
    This considers efficiencies of current and mature technologies today (18-20%), to efficiencies of PV currently in
    development for 2050 (24.5-25%), according to https://science.sciencemag.org/content/352/6283/aad4424/tab-pdf.
    :ivar db: database
    :vartype database: dict
    :ivar year: year
    :vartype year: int

    """

    def __init__(self, db, year, model, scenario):
        self.db = db
        self.year = year
        self.model = model
        self.scenario = scenario
        self.efficiencies = get_efficiency_ratio_solar_PV()

    def update_efficiency_of_solar_pv(self):
        """
        Update the efficiency of solar PV modules.
        We look at how many square meters are needed per kilowatt of installed capacity
        to obtain the current efficiency.
        Then we update the surface needed according to the projected efficiency.
        :return:
        """

        if not os.path.exists(DATA_DIR / "logs"):
            os.makedirs(DATA_DIR / "logs")

        with open(
            DATA_DIR
            / f"logs/log photovoltaics efficiencies change {self.model.upper()} {self.scenario} {self.year}-{date.today()}.csv",
            "w",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            writer.writerow(
                ["dataset name", "location", "technology", "original efficiency", "new efficiency"]
            )

        print(f"Log of changes in photovoltaics efficiencies saved in {DATA_DIR}/logs")

        # to log changes in efficiency
        log_eff = []

        ds = ws.get_many(
            self.db,
            *[
                ws.contains("name", "photovoltaic"),
                ws.either(
                    ws.contains("name", "installation"),
                    ws.contains("name", "construction"),
                ),
                ws.doesnt_contain_any("name", ["market", "factory", "module"]),
                ws.equals("unit", "unit"),
            ],
        )

        for d in ds:
            power = float(re.findall(r"[-+]?\d*\.\d+|\d+", d["name"])[0])

            if "mwp" in d["name"].lower():
                power *= 1000

            for exc in ws.technosphere(
                d,
                *[
                    ws.contains("name", "photovoltaic"),
                    ws.equals("unit", "square meter"),
                ],
            ):

                surface = float(exc["amount"])
                max_power = surface  # in kW, since we assume a constant 1,000W/m^2
                current_eff = power / max_power

                possible_techs = [
                    "micro-Si",
                    "single-Si",
                    "multi-Si",
                    "CIGS",
                    "CIS",
                    "CdTe",
                ]
                pv_tech = [i for i in possible_techs if i.lower() in exc["name"].lower()]

                if len(pv_tech) > 0:
                    pv_tech = pv_tech[0]

                    new_eff = self.efficiencies.sel(technology=pv_tech).interp(
                        year=self.year, kwargs={"fill_value": "extrapolate"}
                    ).values

                    # in case self.year <10 or >2050
                    new_eff = np.clip(new_eff, 0.1, 0.27)

                    # We only update the efficiency if it is higher than the current one.
                    if new_eff > current_eff:
                        exc["amount"] *= float(current_eff / new_eff)
                        d["parameters"] = {
                            "efficiency": new_eff,
                            "old_efficiency": current_eff,
                        }
                        d["comment"] = (
                            f"`premise` has changed the efficiency "
                            f"of this photovoltaic installation "
                            f"from {int(current_eff * 100)} pct. to {int(new_eff * 100)} pt."
                        )
                        log_eff.append([d["name"], d["location"], pv_tech, current_eff, new_eff])

        with open(
            DATA_DIR
            / f"logs/log photovoltaics efficiencies change {self.model.upper()} {self.scenario} {self.year}-{date.today()}.csv",
            "a",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.writer(csv_file, delimiter=";", lineterminator="\n")
            for row in log_eff:
                writer.writerow(row)

        print("Done!")

        return self.db
