from setuptools import find_packages, setup

setup(
    name="itsc2025workshop",
    packages=find_packages(exclude=["itsc2025workshop_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
