import polaris_data
from polaris_data import PolarisClient, _native


def test_production_methods_and_exports():
    assert _native.__native__
    for name in ("health", "openapi", "coverage", "data", "data_rows", "sql", "candles", "stream"):
        assert callable(getattr(PolarisClient, name))
    for name in ("trades", "catalog", "meta", "raw"):
        assert not hasattr(PolarisClient, name)
    for name in polaris_data.__all__:
        assert hasattr(polaris_data, name)
