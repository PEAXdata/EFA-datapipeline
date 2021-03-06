from setuptools import setup, find_packages

setup(
    name="efa_2021_001_30mhz_synchronization",
    version="0.1.123",
    packages=find_packages(),
    include_package_data=True,
    install_requires=["Click", "SQLAlchemy", "pytest", "sentry-sdk"],
    entry_points="""
        [console_scripts]
        efa_30mhz=scripts.sync:cli
    """,
)
