# Material inputs for the remaining polymer recycling chains

Seventeen source-3.8 material correspondences are now admitted for native UVEK
2026 cut-off with ecoinvent 3.10 biosphere. Nine use reconstructed source-market
delivery and eight map directly to complete native services. The direct cases
include single deterministic supplier markets, which need no extra helper.

The two corrugated-box candidates are corrected from base paper to complete
European mixed-fibre, single-wall packaging. Its converting, inks, printing and
glue are included once. The original market delivery entries remain separate.
Fibre mix, wall design, strength, moisture, geography and vintage remain screening
differences with unquantified score effects.

The checked quantity bases are:

| Input | Basis retained |
| --- | --- |
| Soap | One kg wet product: 0.87 kg dry soap and 0.13 kg water; native also states 13% moisture. No concentration correction. |
| Injection moulding | One kg processing service gives 0.994 kg output in both sources. Baseline converted resin is excluded; losses and auxiliaries remain included. No extra yield correction. |
| Plastic-film extrusion | One kg processing service gives 0.976 kg output in both sources. Baseline resin remains separately owned. No extra yield correction. |
| Steel-wire drawing | Processing per kg, excluding baseline wire and coating while retaining native compensatory material/loss and auxiliary inputs. |
| Tin-plated chromium-steel sheet | One square metre, not one kg. Source records 15.6 kg 18/8 steel plus 0.0365 kg tin per m². Native includes steel, rolling and plating once. Original freight is already area-based. |
| Sulfuric acid | One kg 100% acid. Native's 35% burden-free smelter-acid contribution is already in its inventory; no second allocation or dilution correction. |

Ten closed market recipes retain the inspected source producer entries, delivery
amounts and marginal statistics without regrouping or renormalization. Source
producer internals and their allocated burdens are replaced by complete native
models. Retaining producer shares does not preserve the original production
allocation. Generic chemical baskets remain generic baskets; the decisions do
not permit replacing a named chemical with one. All approximations remain in
warnings and review evidence.

One of the ten recipes, the complete source-3.8 acid market, is reserved for the
required recycling parents. It grants no global source-3.8 acid conversion and
leaves the existing source-3.9 manganese dependency unchanged. Its supply vector
was instantiated manually for finite-LCI checks; the actual recycling-parent
conversion remains to be tested when that parent is assembled.

Thirty-two Brightpath checks and six Premise checks pass, including signed
normal/lognormal quantities, both UVEK releases, packaging, area units, cache
replay, permission withdrawal and older physical suppliers. Thirty-four signed
LCIs cover seventeen actual migrated correspondences; two more check the reserved
acid vector. All eighteen independent native-demand comparisons pass without
writing a database or registering an activity. The normal generator reproduces
all three resources, and both biosphere files remain unchanged.

The source-3.6 organic-basket permission is preserved from its existing approved
positive-reference record. That licensed project is not installed locally, so
fresh source-3.6 extraction was not rerun. Tests verify that the existing
authorization still works after the new source-3.8 permissions are withdrawn.
All new source-3.8 decisions use locally inspected source data.

The required-input diagnostic falls from 76 to 59 unapproved correspondences:
57 utility/waste cases plus the reserved rail and acid components. It remains a
component diagnostic; both top-level recycling markets and the truck update
remain unfinished. Fourteen of nineteen sectors remain qualified.
