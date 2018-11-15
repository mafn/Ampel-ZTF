from setuptools import setup
setup(name='Ampel-ZTF',
      version='0.5.0',
      package_dir={'':'src'},
      package_data = {'': ['*.json']},
      packages=[
          'ampel.ztf.archive',
          'ampel.ztf.pipeline.common',
          'ampel.ztf.pipeline.t0',
          'ampel.ztf.pipeline.t0.load',
          'ampel.ztf.pipeline.t0.ingest',
          'ampel.ztf.pipeline.t3.sergeant',
      ],
      entry_points = {
			'console_scripts' : {
				'ampel-ztf-alertprocessor = ampel.ztf.pipeline.t0.run:run_alertprocessor'
			},
			'ampel.pipeline.t0.sources' : {
				'ZTFIPAC = ampel.ztf.pipeline.t0.ZISetup:ZISetup',
			}
      }
)
