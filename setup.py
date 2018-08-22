from setuptools import setup
setup(name='Ampel-ZTF',
      version='0.4.0',
      package_dir={'':'src'},
      package_data = {'': ['*.json']},
      packages=[
          'ampel.archive',
          'ampel.pipeline.t0',
          'ampel.pipeline.t0.alerts',
          'ampel.pipeline.t0.ingesters',
      ],
      entry_points = {
      }
)
