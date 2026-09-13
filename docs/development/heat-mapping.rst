Heat mapping maintenance
==========================

Maintaining or extending the mapping
--------------------------------------

``premise/iam_variables_mapping/heat.yaml`` is the authoritative mapping. When
adding an IAM or technology:

#. choose the physical layer before choosing an inventory proxy;
#. state whether the IAM series is final energy or heat output;
#. select the conversion rule and an exact ecoinvent activity filter;
#. add an explicit ``assumption`` for any proxy or aggregation choice;
#. use a signed ``terms`` expression only when the IAM relationship supports it;
#. add tests for complete, absent, and partially available layers; and
#. test both market shares and the absence of generated-market cycles.

The source files most relevant to this workflow are:

* ``premise/iam_variables_mapping/heat.yaml`` for IAM and inventory mappings;
* ``premise/heat_data.py`` for expression evaluation and layer availability;
* ``premise/heat.py`` for conversion, market creation, and relinking.
