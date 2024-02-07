# kafka_commedy_club
Using Apache Kafka in combination with Cassandra, FastAPI and Flask/Dash to create a trading dashboard.


# Instructions (Windows)


## General
Install WSL2, docker-desktop.

```
Docker-compose up
```

Delete all existing docker images and data:
```
./scripts/clean_setup.ps1
```

Create virtual environment
```
python3 -m venv venv
```

Activate and install package localy, also installs requirements.

```
venv/Scripts/activate
pip install -e .
```

## Dev

```
pip install .[dev]
```

This command tells pip to install the current package (denoted by the .) along with the dependencies listed under the dev key in extras_require. If you are in a directory with a setup.py file, this will install the package itself in editable mode (-e), along with the extra dependencies specified for development.

# Docker

# Todo
