from setuptools import setup, find_packages


setup(name="plc-setting",
      version="0.0.1",
      description="OPC-UA Address Space Modeler",
      author="Olivier R-D et al.",
      url='https://github.com/Huanghui52/opcua-modeler',
      packages=["uamodeler"],
      license="GNU General Public License",
      install_requires=["opcua>=0.95.1", "opcua-widgets>=0.5.10"],
      entry_points={'console_scripts':
                    ['opcua-modeler = uamodeler.uamodeler:main']
                    }
      )
