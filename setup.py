from setuptools import setup, find_packages

setup(
    name='efa_2021_001_30mhz_synchronization',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
        'SQLAlchemy',
    ],
    entry_points='''
        [console_scripts]
        efa_30mhz=scripts.sync:cli
    ''',
)
