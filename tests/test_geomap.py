import pytest
from rmnd_lca.geomap import Geomap

geomap = Geomap()

def test_ecoinvent_to_REMIND():
    # DE is in EUR
    assert geomap.ecoinvent_to_remind_location("DE") == "EUR"
    # CN is in CHA
    assert geomap.ecoinvent_to_remind_location("CN") == "CHA"

def test_REMIND_to_ecoinvent():
    # DE and CH are in EUR (at least for now)
    assert "DE" in geomap.remind_to_ecoinvent_location("EUR")
    assert "CH" in geomap.remind_to_ecoinvent_location("EUR")
    # Hongkong is in China (really?)
    assert "HK" in geomap.remind_to_ecoinvent_location("CHA")
    # in Japan there is only JP
    assert ["JP"] == geomap.remind_to_ecoinvent_location("JPN")
