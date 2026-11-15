Synthetic scenario-delta-v1 reader fixture
========================================

This bundle was generated once with the V1 writer from Premise commit 41bfc51f,
before that obsolete writer was removed. It contains no ecoinvent data.

source.inventory-store contains a provider and a consumer. The scenario removes
the consumer's comment, changes custom metadata, patches a technosphere amount
from 2 to 3.25, adds an exchange of 0.5, and adds a third activity. The V1 stream
therefore covers source ranges, patched exchanges, added exchanges, and both
patched and added activity records. expected.json records the normalized result.

The bundles use schema 10; their manifests, checksums and relative base reference
are part of the fixture. Tests copy the whole directory to verify relocation and
read the historical stream through the current reader and scenario loader.
Do not regenerate it with a newer writer: it is a backward-reading contract.
