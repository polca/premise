Supplier-selection implementation
===================================

This decision tree describes helpers in ``premise/transformation.py``.
The methodological explanation is in :doc:`/methodology/regionalization`;
these helper functions are not the public integration API.

.. _decision-tree:

**Decision Tree for Processing Datasets**


The process begins with a dataset that requires processing.


Decision: Is the Exchange in Cache?
-------------------------------------

- **Yes**

  - Use ``process_cached_exchange``.

    - Retrieve cached data.
    - Update ``new_exchanges`` with cached data.

- **No**

  - Use ``process_uncached_exchange``.

    **Decision: Number of Possible Datasets**

    - **None**

      - Print a warning and return.

    - **One**

      - Use ``handle_single_possible_dataset``.

        - Use the single matched dataset.
        - Update ``new_exchanges`` with this dataset information.

    - **Multiple**

      - Use ``handle_multiple_possible_datasets``.

        **Decision: Does Dataset Location Match Possible Dataset Locations?**

        - **Yes**

          - Use the matched dataset location.

        - **No**

          - Use ``process_complex_matching_and_allocation``.

            **Decision: Dataset Location Type**

            - **IAM Region**

              - Use ``handle_iam_region``.

                - Match IAM region to ecoinvent locations.
                - Update ``new_exchanges`` with IAM region-specific data.
                - Cache the new entry.

            - **Global ('GLO', 'RoW', 'World')**

              - Use ``handle_global_and_row_scenarios``.

                - Allocate inputs for global datasets.
                - Update ``new_exchanges`` with global data.
                - Cache the new entry.

            - **Others**

              - Perform GIS matching.

                - Determine intersecting locations with GIS.
                - Allocate inputs based on GIS matches.
                - Update ``new_exchanges`` with GIS-specific data.
                - Cache the new entry.

Final Steps
-------------

- If no match is found, use ``handle_default_option``.

  - Integrate new exchanges into the dataset.
