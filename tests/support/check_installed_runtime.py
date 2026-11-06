"""Check an installed Premise artifact and its numerical/export runtime."""

from importlib.metadata import requires, version
import os
from pathlib import Path
import sys
import tempfile

from packaging.requirements import Requirement


def main():
    # Set this before importing Premise or Brightway; never touch user projects.
    os.environ["BRIGHTWAY2_DIR"] = tempfile.mkdtemp(prefix="premise-install-check-")

    import bw2calc as bc
    import bw2data as bd
    import numpy as np
    import pyarrow as pa
    import premise
    from premise import NewDatabase  # noqa: F401
    from premise.simapro_export import check_brightpath
    from brightpath.formats.openlca_methods import OpenLCAMethodMapping  # noqa: F401

    assert Path(premise.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    for specification in requires("premise") or ():
        requirement = Requirement(specification)
        if requirement.marker and not requirement.marker.evaluate({"extra": ""}):
            continue
        installed = version(requirement.name)
        assert installed in requirement.specifier, (specification, installed)
    assert pa.array(np.array([1.0])).to_pylist() == [1.0]
    check_brightpath()

    bd.projects.set_current("installation-check")
    flow = ("installation-biosphere", "emission")
    bd.Database(flow[0]).write(
        {flow: {"name": "test emission", "unit": "kilogram", "type": "emission"}}
    )
    process = ("installation-processes", "process")
    database = bd.Database(process[0])
    database.write(
        {
            process: {
                "name": "test process",
                "reference product": "test product",
                "unit": "kilogram",
                "location": "GLO",
                "exchanges": [
                    {"input": process, "type": "production", "amount": 1},
                    {"input": flow, "type": "biosphere", "amount": 2},
                ],
            }
        }
    )
    method = bd.Method(("installation-check", "test indicator"))
    method.register(unit="kilogram")
    method.write([(flow, 3)])
    lca = bc.LCA({database.get(process[1]): 1}, method.name)
    lca.lci()
    lca.lcia()
    assert abs(lca.score - 6) < 1e-12
    print("Installed artifact, requirements, exporters and Brightway LCIA passed.")


if __name__ == "__main__":
    main()
