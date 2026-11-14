Inventory migration between ecoinvent versions
================================================

Additional inventories can have been authored for a different ecoinvent
version from your source database. Premise uses migration maps to translate
changed supplier or flow identities so those inventories can link to the
target version.

The **import source version** describes the inventory's original references;
the **target version** is the ecoinvent version used by your build. They need
not be the same. Migration changes the relevant identities, not the scientific
assumptions of every process. Suppliers and background burdens can still
differ between versions.

See the versioned `migration maps <https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/utils/import/migrations>`__
and :doc:`inventories` for source selection. Sector chapters describe
important substitutions and remaining gaps.

.. _native-en15804-inventory-links:

EN15804 suppliers and exports
-----------------------------

EN15804 uses attributional IAM market shares and retains secondary-material
technologies. Its supplemental inventories use the attributional source
selection, with explicit native EN15804 scrap suppliers checked before linking.
New aluminium scrap uses the RoW market as a geographic proxy; its positive
reference output credits the existing negative scrap handoff. Post-consumer
aluminium and iron use negative-output treatment markets. Copper scrap uses
the native burden-free provider. Signed quantities and uncertainty are retained.
These adaptations do not add a separate recovery credit or certify an EPD.

EN15804 caches and exported metadata retain their system-model identity.
Brightway, matrix, datapackage and openLCA exports retain this identity.
SimaPro export requires a Brightpath version with EN15804 naming support;
versions supporting only cut-off and consequential naming raise an error
before scenario preparation or file publication.

The native supplier contracts are verified against ecoinvent 3.12 EN15804.
An absent, ambiguous or incompatible native scrap supplier raises an error.


Supplemental inventory proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For consequential imports, the ASSB recycled HDPE purchase uses the native
marginal HDPE market at the same mass: GLO through 3.11, and RER in 3.12 to
retain European supply. Cut-off and EN15804 retain the recycled supplier.

Two chemical proxies apply only to ecoinvent 3.8, where the native markets
are unavailable. The polymer ASSB uses the bundled Japanese lithium iron
phosphate synthesis recipe. The iridium inventory uses the GLO organic
chemical market as a manufacturing-burden proxy for sodium ethyl xanthate.
Both retain the original kg quantities. The latter is a weaker proxy and
does not establish chemical or environmental equivalence. Imported exchange
comments identify these substitutions; later versions use the native markets.
