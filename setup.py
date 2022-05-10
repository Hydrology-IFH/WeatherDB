from setuptools import setup, find_packages
import os
os.environ["WEATHERDB_MODULE_INSTALLING"] = "True"
import weatherDB
os.environ.pop("WEATHERDB_MODULE_INSTALLING")

def readme():
    with open('README.md') as f:
        return f.read()

def requirements():
    with open('requirements.txt') as f:
        return f.readlines()

setup(
   name='weatherDB',
   version=weatherDB.__version__,
   description='This is a package to work with the Weather Database.',
   long_description=readme(),
   classifiers=[
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Scientific/Engineering :: Hydrology',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        'Intended Audience :: Science/Research',
        'Natural Language :: English'
      ],
   author='Max Schmit',
   author_email='max.schmit@hydrology.uni-freiburg.de',
   url='https://github.com/maxschmi/WeatherDB_module',
   license='GNU GPT3',
   packages=find_packages(include=['weatherDB', 'weatherDB.*']), 
   install_requires=requirements(),
   python_requires='>=3.6, <4',
)