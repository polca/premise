"""Executable arithmetic example; illustrative values, not measured PV scores."""

import math


def main():
    old_efficiency, new_efficiency = 0.40, 0.44
    old_fuel = 3.6 / old_efficiency
    new_fuel = old_fuel * old_efficiency / new_efficiency
    assert math.isclose(new_fuel, 3.6 / new_efficiency)
    print(f"Fuel per kWh: {old_fuel:.3f} -> {new_fuel:.3f} MJ")

    # PV sensitivity holds installation burdens fixed; operation is excluded.
    lifetime = 30
    burden_per_kwp = 1000  # illustrative kg CO2-eq/kWp, not an inventory result
    for annual_yield in (1000, 1300):
        score = 1000 * burden_per_kwp / (lifetime * annual_yield)
        print(f"Yield {annual_yield}: {score:.2f} g CO2-eq/kWh")
    assert math.isclose((1000 / 1300), 0.7692307692307693)


if __name__ == "__main__":
    main()
