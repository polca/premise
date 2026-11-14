IAM model comparison
======================

.. raw:: html

   <span id="choosing-the-right-iam"></span>


.. contents:: On this page
   :local:
   :depth: 1

These model descriptions help you choose which projections to examine.
An IAM's general capabilities do not tell you which variables Premise uses
from a particular file. Check :doc:`coverage` alongside this comparison, then
follow :doc:`/user_guide/choose-scenario` to configure a build.

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Model
     - Description
   * - `REMIND <https://www.pik-potsdam.de/en/institute/departments/transformation-pathways/models/remind>`__
     - REMIND (Regionalized Model of Investment and Development) links economic growth, energy investment and climate policy. It models how regions invest in technologies and energy resources under climate constraints.
   * - REMIND-EU
     - REMIND-EU represents European energy systems and policies in more regional detail than the global REMIND model.
   * - `IMAGE <https://models.pbl.nl/image/Index.php>`__
     - IMAGE (Integrated Model to Assess the Global Environment) links energy, land use, agriculture and climate. It represents environmental processes alongside changes in population and economic activity.
   * - `TIAM-UCL <https://www.ucl.ac.uk/energy-models/sites/energy_models/files/tiam-ucl-manual.pdf>`__
     - TIAM-UCL (TIMES Integrated Assessment Model by University College London) models energy technologies, resources and supply chains. It uses the TIMES framework to examine the costs and technology choices of decarbonization pathways.
   * - `MESSAGE <https://docs.messageix.org/>`__
     - MESSAGEix-GLOBIOM-GAINS (MESSAGE) couples the MESSAGEix energy system with the GLOBIOM land-use model and GAINS air-pollution module. It is used to explore long-term energy and land-use transitions and their climate and air-quality implications under different policy scenarios.
   * - `GCAM <https://gcims.pnnl.gov/modeling/gcam-global-change-analysis-model>`__
     - GCAM (Global Change Analysis Model) links energy, water, land use, climate and economic systems. It represents their responses to policies, technology changes and socioeconomic assumptions.


.. raw:: html

   <span id="quick-reference"></span>

Choosing an IAM
-----------------

Start with the question your study needs to answer. The descriptions above
outline the models' general focus; :doc:`coverage` shows which sectors have
configured Premise mappings. More mapped variables do not by themselves mean
better inventories or more reliable projections.

For a comparison between models, use pathways with compatible assumptions
and compare the same year, service, background system model and LCIA method.
Even then, differences can reflect technology definitions, geography and data
coverage as well as policy assumptions.

Check the actual data
----------------------

1. Confirm that the selected model/pathway file is available for your Premise
   release and includes the requested year.
2. Check the reported variables for the sectors important to your study.
   A mapping in the code still needs data in that file.
3. Read the relevant :doc:`sector chapters </methodology/index>` for external
   data, fallback assumptions and technology boundaries.
4. Review the scenario-variable summary and validation coverage after building;
   see :doc:`/user_guide/reports`.

Use :doc:`/user_guide/choose-scenario` to configure the selected scenario and
:doc:`scenarios` to look up pathway descriptions. The generated mapping table
in :doc:`coverage` is the maintained reference for sector aliases.
