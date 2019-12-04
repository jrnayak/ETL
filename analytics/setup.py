"""
Generates EGG file for PMO
"""
from setuptools import setup, find_packages

setup(
    name='onfido',
    version="0.1",
    description='Test',
    packages=find_packages(exclude=["*.tests"]),
    scripts=['zmain.py'],
    data_files={'config/onfido.cfg'},
    # packages=to scan
    zip_safe=False
)
