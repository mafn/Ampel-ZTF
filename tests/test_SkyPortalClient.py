import pytest
from pydantic import ValidationError

from ampel.secret.DictSecretProvider import NamedSecret
from ampel.ztf.t3.skyportal.SkyPortalClient import SkyPortalClient


def test_validate_url():
    """URL path may not be set"""
    with pytest.raises(ValidationError):
        SkyPortalClient.validate(
            base_url="http://foo.bar/", token=NamedSecret(label="foo", value="seekrit")
        )
    SkyPortalClient.validate(
        base_url="http://foo.bar", token=NamedSecret(label="foo", value="seekrit")
    )
