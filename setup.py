#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-ZTF/setup.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : Unspecified
# Last Modified Date: 19.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from setuptools import setup, find_namespace_packages

package_data = {
	'conf': [
		'ampel-ztf/*.yaml', 'ampel-ztf/*.yml', 'ampel-ztf/*.json',
		'ampel-ztf/**/*.yaml', 'ampel-ztf/**/*.yml', 'ampel-ztf/**/*.json',
	],
	'ampel.test': ['test-data/*']
}


extras_require = {
	'archive': ['ampel-ztf-archive>=0.7.0-alpha.0']
}

setup(
    name = 'ampel-ztf',
    version = '0.8.1a2',
    description = 'Zwicky Transient Facility support for the Ampel system',
    author = 'Valery Brinnel',
    author_email = None,
    maintainer = 'Jakob van Santen',
    maintainer_email = 'jakob.van.santen@desy.de',
    url = 'https://ampelproject.github.io',
    packages = find_namespace_packages(),
    package_data = package_data,
    extras_require = extras_require,
    python_requires = '>=3.8,<4.0'
)
