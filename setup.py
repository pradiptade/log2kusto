# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


setup(
    name='logging_kusto',
    version='0.1.0',
    description='Logging module that provides writing to Kusto database',
    long_description=open('README.md').read(),
    author='',
    author_email='',
    url='',
    packages=find_packages(exclude=('tests'))
)
