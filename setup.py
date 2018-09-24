from setuptools import setup
setup(name='Ampel-ZTF',
      version='0.5.0',
      package_dir={'':'src'},
      package_data = {'': ['*.json']},
      packages=[
          'ampel.archive',
          'ampel.pipeline.common',
          'ampel.pipeline.t0',
          'ampel.pipeline.t0.load',
          'ampel.pipeline.t0.ingest',
          'ampel.pipeline.t3.sergeant',
      ],
      entry_points = {
			'ampel.pipeline.t0.sources' : {
				'ZTFIPAC = ampel.pipeline.t0.ZISetup:ZISetup',
			}
      }
)
