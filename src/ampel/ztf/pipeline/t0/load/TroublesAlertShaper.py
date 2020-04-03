
from ampel.ztf.pipeline.common.ZTFUtils import ZTFUtils
from types import MappingProxyType

class TroublesAlertShaper:
    @staticmethod 
    def shape(in_dict):
        alert = in_dict['alert']
        return {
            'pps': alert['pps'],
            'ro_pps': tuple(map(MappingProxyType,alert['pps'])),
            'uls': in_dict['alert']['uls'],
            'ro_uls': tuple(map(MappingProxyType,alert['uls'])) if alert['uls'] else None,
            'tran_id': in_dict['tranId'],
            'ztf_id': ZTFUtils.to_ztf_id(in_dict['tranId']),
            'alert_id': alert['id']
        }