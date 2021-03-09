import pytest
from pydantic import ValidationError

from ampel.dev.DictSecretProvider import SecretWrapper
from ampel.ztf.t3.skyportal.SkyPortalClient import SkyPortalClient


def test_validate_url():
    """URL path may not be set"""
    with pytest.raises(ValidationError):
        SkyPortalClient.validate(
            base_url="http://foo.bar/", token=SecretWrapper(key="foo", value="seekrit")
        )
    SkyPortalClient.validate(
        base_url="http://foo.bar", token=SecretWrapper(key="foo", value="seekrit")
    )
