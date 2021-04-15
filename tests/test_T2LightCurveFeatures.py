
import pytest
import logging
from pathlib import Path
from ampel.ztf.t2.T2LightCurveFeatures import T2LightCurveFeatures
from ampel.ztf.dev.ZTFAlert import ZTFAlert

from ampel.ztf.alert.ZiAlertSupplier import ZiAlertSupplier
from ampel.alert.load.FileAlertLoader import FileAlertLoader
from ampel.alert.load.TarAlertLoader import TarAlertLoader


@pytest.fixture
def lightcurve():
    path = str(Path(__file__).parent/"test-data"/"ZTF20abyfpze.avro")

    supplier = ZiAlertSupplier(deserialize="avro")
    supplier.set_alert_source(FileAlertLoader(files=[path]))
    return ZTFAlert.to_lightcurve(pal=next(supplier))

def test_features(lightcurve):
    t2 = T2LightCurveFeatures(logger=logging.getLogger())
    result = t2.run(lightcurve)
    for prefix in t2.extractor.names:
        assert any(k.startswith(prefix) for k in result)
