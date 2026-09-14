from copy import deepcopy
from types import SimpleNamespace

import pytest
from stats_arrays import UncertaintyBase, uncertainty_choices

from premise.fuels.base import Fuels
from premise.fuels.carbon import reclassify_fuel_co2

FLOW = {('Carbon dioxide, non-fossil', 'air', 'unspecified', 'kilogram'): 'bio'}


def emission(name, amount, **kwargs):
    return dict(name=name, amount=amount, unit='kilogram', type='biosphere',
                categories=('air',), **kwargs)


def amounts(ds):
    return {name: sum(e['amount'] for e in ds['exchanges'] if e['name'] == name)
            for name in ('Carbon dioxide, fossil', 'Carbon dioxide, non-fossil')}


@pytest.mark.parametrize('share', [0, .01, .45, 1])
@pytest.mark.parametrize('captured', [0, .36184272170066833])
def test_capture_balance_and_repeatability(share, captured):
    gross = .40204746648669243
    stack = gross - captured
    capture = dict(name='carbon dioxide, captured from natural gas', type='technosphere',
                   amount=captured, unit='kilogram')
    ds = {'exchanges': [emission('Carbon dioxide, fossil', stack), capture]}
    for _ in range(2):
        reclassify_fuel_co2(ds, .20681454241275787 * 2.12, share, FLOW, 'gas')
        result = amounts(ds)
        assert result['Carbon dioxide, fossil'] == pytest.approx(stack * (1-share))
        assert result['Carbon dioxide, non-fossil'] == pytest.approx(stack * share)
        assert sum(result.values()) + capture['amount'] == pytest.approx(gross)
        assert capture['amount'] == captured
    assert sum(e['name'] == 'Carbon dioxide, non-fossil' for e in ds['exchanges']) <= 1


def test_preserves_other_biogenic_carbon_and_replaces_prior_share():
    bio = emission('Carbon dioxide, non-fossil', 3)
    ds = {'exchanges': [emission('Carbon dioxide, fossil', 10), bio]}
    for share in (.5, .25, 1, 0):
        reclassify_fuel_co2(ds, 2, share, FLOW, 'liquid')
        assert amounts(ds)['Carbon dioxide, fossil'] == pytest.approx(10 - 2*share)
        assert bio['amount'] == pytest.approx(3 + 2*share)
        assert len(ds['exchanges']) == 2


def test_two_fuels_do_not_overwrite_each_other():
    ds = {'exchanges': [emission('Carbon dioxide, fossil', 10)]}
    for _ in range(2):
        reclassify_fuel_co2(ds, 2, .5, FLOW, 'diesel')
        reclassify_fuel_co2(ds, 3, .5, FLOW, 'gas')
    assert amounts(ds) == pytest.approx({'Carbon dioxide, fossil': 7.5,
                                       'Carbon dioxide, non-fossil': 2.5})


def test_compartments_negative_flows_and_uncertainty():
    fossil = emission('Carbon dioxide, fossil', .04,
                     **{'uncertainty type': 5, 'loc': .04, 'minimum': .02, 'maximum': .06})
    fossil2 = deepcopy(fossil)
    fossil2['categories'] = ('air', 'urban air close to ground')
    uptake = emission('Carbon dioxide, fossil', -.2)
    ds = {'exchanges': [fossil, fossil2, uptake]}
    flow = {**FLOW, ('Carbon dioxide, non-fossil', 'air', 'urban air close to ground', 'kilogram'): 'urban'}
    reclassify_fuel_co2(ds, .4, .45, flow, 'gas')
    assert fossil['amount'] == pytest.approx(.022)
    assert fossil['loc'] == pytest.approx(.022)
    assert fossil['minimum'] == pytest.approx(.011)
    assert fossil['maximum'] == pytest.approx(.033)
    assert uptake['amount'] == -.2
    assert {e['input'][1] for e in ds['exchanges'] if e['name']=='Carbon dioxide, non-fossil'} == {'bio','urban'}
    uncertainty_choices[5].validate(UncertaintyBase.from_dicts(dict(
        uncertainty_type=5, loc=fossil['loc'], minimum=fossil['minimum'], maximum=fossil['maximum'])))


def test_zero_endpoint_removes_invalid_lognormal_parameters():
    fossil = emission('Carbon dioxide, fossil', .04,
                     **{'uncertainty type': 2, 'loc': -3.2, 'scale': .2})
    ds = {'exchanges': [fossil]}
    reclassify_fuel_co2(ds, .4, 1, FLOW, 'gas')
    assert fossil['amount'] == fossil['loc'] == 0
    assert fossil['uncertainty type'] == 0
    assert 'scale' not in fossil
    reclassify_fuel_co2(ds, .4, 0, FLOW, 'gas')
    assert fossil['amount'] == pytest.approx(.04)


def gas_fixture(share=.45):
    ds = dict(name='CCS plant', location='R1', exchanges=[
        dict(name='market group for natural gas, high pressure', product='natural gas, high pressure',
             location='GLO', unit='cubic meter', amount=.20681454241275787, type='technosphere'),
        emission('Carbon dioxide, fossil', .040204744786024094)])
    fuels=object.__new__(Fuels)
    fuels.database=[ds]
    fuels.fuel_map={'natural gas': [], 'methane, from biomass': []}
    fuels.iam_data=SimpleNamespace(production_volumes=None)
    fuels.regions=['R1']
    fuels.ecoinvent_to_iam_loc={}
    fuels.biosphere_flows=FLOW
    fuels.is_in_index=lambda exc, loc: True
    fuels.get_technology_and_regional_production_shares=lambda **kwargs: (
        None, {('natural gas','R1'):1-share, ('methane, from biomass','R1'):share}, {'R1':1})
    return fuels, ds


@pytest.mark.parametrize('share',[.01,.45])
def test_gas_mixin_reproduces_corrected_2020_2050(share):
    fuels, ds = gas_fixture(share)
    fuels.update_carbon_dioxide_emissions()
    fuels.update_carbon_dioxide_emissions()
    assert amounts(ds)['Carbon dioxide, non-fossil'] == pytest.approx(.040204744786024094*share)
    assert sum(amounts(ds).values()) == pytest.approx(.040204744786024094)


def test_coal_methane_is_not_biogenic_and_small_shares_are_not_rounded():
    fuels, ds=gas_fixture()
    fuels.fuel_map['methane, from coal']=[]
    fuels.get_technology_and_regional_production_shares=lambda **kwargs: (
        None, {('natural gas','R1'):.5, ('methane, from coal','R1'):.496,
               ('methane, from biomass','R1'):.004}, {'R1':1})
    fuels.update_carbon_dioxide_emissions()
    assert amounts(ds)['Carbon dioxide, non-fossil'] == pytest.approx(.040204744786024094*.004)


def test_liquid_ccs_uses_residual_emissions():
    fuels, ds=gas_fixture()
    ds['exchanges'][0].update(name='market for diesel', unit='kilogram', amount=.1)
    fuels.fuel_map={'diesel':[], 'biodiesel':[]}
    fuels.get_technology_and_regional_production_shares=lambda **kwargs: (
        None, {('diesel','R1'):.55, ('biodiesel','R1'):.45}, {'R1':1})
    for _ in range(2):
        fuels.update_fuel_carbon_dioxide_emissions(
            ['diesel','biodiesel'], ['market for diesel'],3.15,['diesel'])
    assert amounts(ds)['Carbon dioxide, non-fossil'] == pytest.approx(.040204744786024094*.45)


def test_compact_exchanges_do_not_duplicate_biogenic_flow():
    from premise.inventory_store import compact_exchange_payload
    fossil = compact_exchange_payload(emission('Carbon dioxide, fossil', .04))
    bio = compact_exchange_payload(emission('Carbon dioxide, non-fossil', .01))
    ds = {'exchanges': [fossil, bio]}
    for _ in range(2):
        reclassify_fuel_co2(ds, .4, .45, FLOW, 'gas')
    assert len(ds['exchanges']) == 2
    assert amounts(ds) == pytest.approx({'Carbon dioxide, fossil': .022,
                                       'Carbon dioxide, non-fossil': .028})


def test_gas_share_uses_remapped_supplier_region():
    fuels, ds = gas_fixture()
    # Consumer location is an ecoinvent alias; the actual linked supplier is R1.
    ds['location']='CH'
    fuels.ecoinvent_to_iam_loc={'CH':'R1'}
    fuels.update_carbon_dioxide_emissions()
    assert ds['exchanges'][0]['location']=='R1'
    assert amounts(ds)['Carbon dioxide, non-fossil']==pytest.approx(.040204744786024094*.45)


def test_zero_world_weights_do_not_divide_by_zero():
    fuels, ds = gas_fixture()
    fuels.get_technology_and_regional_production_shares=lambda **kwargs: (
        None, {('natural gas','R1'):.55, ('methane, from biomass','R1'):.45}, {})
    fuels.update_carbon_dioxide_emissions()
    assert sum(amounts(ds).values())==pytest.approx(.040204744786024094)
