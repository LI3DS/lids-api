# -*- coding: utf-8 -*-
import os
import re
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

requirements = (
    'flask==0.11.1',
    'flask-restplus==0.10.0',
    'psycopg2==2.7.3',
    'pyyaml',
    'graphviz>=0.5.1'
)

dev_requirements = (
    'pytest',
    'pytest-flask',
    'tox',
    'flake8',
    'invoke',
    'pytest-cov',
)

doc_requirements = (
    'sphinx',
    'sphinx_rtd_theme',
)

prod_requirements = (
    'uwsgi'
)


def find_version(*file_paths):
    """
    see https://github.com/pypa/sampleproject/blob/master/setup.py
    """
    with open(os.path.join(here, *file_paths), 'r') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string. "
                       "Should be at the first line of __init__.py.")


setup(
    name='api_li3ds',
    version=find_version('api_li3ds', '__init__.py'),
    description="REST Api for li3ds metadata",
    url='https://github.com/LI3DS/api-li3ds',
    author='dev',
    author_email='contact@oslandia.com',
    license='GPLv3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    extras_require={
        'dev': dev_requirements,
        'prod': prod_requirements,
        'doc': doc_requirements
    }
)
