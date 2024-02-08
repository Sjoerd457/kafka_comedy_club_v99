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


Run

```
python .main.py
```

Sometimes this gives issues, as the Cassandra Keyspace doesn't exist.
Then run:

```
./src/producers/example_producer.py
```

After that, try main again. Now open 127.0.0.1:8050 in your browser.

## Dev

```
pip install .[dev]
```

This command tells pip to install the current package (denoted by the .) along with the dependencies listed under the dev key in extras_require. If you are in a directory with a setup.py file, this will install the package itself in editable mode (-e), along with the extra dependencies specified for development.

# Docker-compose

The Docker Compose file orchestrates a development environment featuring Apache Kafka and Cassandra, along with essential components for schema management and data ingestion.

## Services Overview

- **ZooKeeper (`zk`)**: Manages Kafka cluster state. Runs on port 2181.

- **Kafka Brokers (`broker0`, `broker1`, `broker2`)**: Form the Kafka messaging backbone. Each broker is exposed on both internal and host-accessible ports (29092-29094 for internal, 9092-9094 for host).

- **Schema Registry (`schema-registry`)**: Manages Avro schemas for Kafka messages, facilitating message serialization and deserialization. Accessible on port 8081.

- **Kafka Connect (`kafka-connect`)**: Integrates Kafka with external data sources and sinks, such as Cassandra. Exposed on port 8083 for REST API interactions.

- **CLI Tools (`cli-tools`)**: Provides Kafka command-line tools within the Docker environment for administrative tasks.

- **Cassandra (`cassandra`)**: Stores large sets of data across multiple nodes without a single point of failure. Cassandra's service runs on port 9042.

- **Cassandra Shell (`cassandra-shell`)**: Offers a CLI environment for interacting directly with Cassandra.

## Noteworthy Configuration Details

- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` and `KAFKA_ADVERTISED_LISTENERS` are configured to ensure communication between Kafka brokers and clients both within and outside the Docker network.

- `KAFKA_MIN_INSYNC_REPLICAS` and `KAFKA_DEFAULT_REPLICATION_FACTOR` are set to enhance data durability and fault tolerance.

- `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` links the Schema Registry to the Kafka brokers, ensuring it can store and retrieve message schemas.

- Kafka Connect's `CONNECT_PLUGIN_PATH` includes paths to directories containing connectors and plugins, enhancing its capabilities to interface with various data systems.

## Networking

- All services are connected via a custom Docker network named `localnet`, facilitating seamless internal communication while isolating the setup from the host network.


# Todo

- Use Confluent Schema Registry with Apache Avro
    - Adjust library of kafka python into confluent verion
    - Create schema
    - Create Afro ...
    - Integrate in producer
    - Integrate in consumer
- Setup Sink Connector Cassandra
    - ...
- Update README
    - Describe containers
    - Describe cli-tools
    - Reason Kafka
    - Reason Cassandra
    - ML Engineering
- Tests
    - ...
