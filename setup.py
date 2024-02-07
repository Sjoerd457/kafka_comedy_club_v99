from setuptools import setup, find_packages


# Function to read the list of requirements from requirements.txt
def read_requirements():
    with open('requirements.txt', 'r') as req:
        return req.read().splitlines()


setup(
    name='kafka_comedy_club_v99',
    version='0.1.0',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest",
            "pytest-asyncio",
        ],
    },
)
