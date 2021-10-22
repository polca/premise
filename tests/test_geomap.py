from premise.geomap import Geomap

geomap = Geomap(model="remind")


def test_ecoinvent_to_REMIND():
    # DE is in EUR
    assert geomap.ecoinvent_to_iam_location("DE") == "EUR"
    # CN is in CHA
    assert geomap.ecoinvent_to_iam_location("CN") == "CHA"


def test_REMIND_to_ecoinvent():
    # DE and CH are in EUR and NEU
    assert "DE" in geomap.iam_to_ecoinvent_location("EUR")
    assert "CH" in geomap.iam_to_ecoinvent_location("NEU")
    # Hongkong is in China (really?)
    assert "HK" in geomap.iam_to_ecoinvent_location("CHA")
    # Japan is in JPN
    assert "JP" in geomap.iam_to_ecoinvent_location("JPN")


def test_REMIND_to_ecoinvent_contained():
    # RU is not contained in EUR
    assert "RU" not in geomap.iam_to_ecoinvent_location("EUR", contained=True)
