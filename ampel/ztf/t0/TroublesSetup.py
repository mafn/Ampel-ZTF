
from .ZISetup import ZISetup, AlertSupplier
from .load.TroublesAlertShaper import TroublesAlertShaper

class TroublesSetup(ZISetup):
    def get_alert_supplier(self, alert_loader):
        """ 
        :param alert_loader: iterable instance that returns the content of alerts
        :returns: instance of ampel.pipeline.t0.load.AlertSupplier
        """
        return AlertSupplier(
            alert_loader, TroublesAlertShaper.shape, serialization=None
        )