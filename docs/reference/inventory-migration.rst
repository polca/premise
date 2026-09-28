Inventory migration between ecoinvent versions
================================================

Because the additional inventories that are imported may be composed
of exchanges meant to link with an ecoinvent version different
than what the user specifies to *premise* upon the database creation,
it is necessary to be able to "translate" the imported inventories
so that they correctly link to any ecoinvent version *premise* is
compatible with.

Therefore, *premise* has a migration map that is used to convert
certain exchanges to be compatible with a given ecoinvent version.

The versioned migration maps are provided here: `migrationmap <https://github.com/polca/premise/tree/76dbf845ef73bb765024dda1143960a24964a5fe/premise/data/utils/import/migrations>`__.
