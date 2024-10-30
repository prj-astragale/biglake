# *Inlake*: a knowledge-base gateway

*Inlake* is a data gateway for an *event-centric* knowledge-base system. It is intended for the smooth ingestion of: binary files and their metadatas, structured and unstructured textual information.

**Motivation:** This small API development provides some flexibility while adressing the requirements for data ingestion. *Inlake* is a part of the *KIBOS* architecture set up for the *Astragale* scientific project.

## Features
+ [Avro](https://avro.apache.org/) data verification and validation
+ S3 Storage for binaries
+ [Apache Kafka](https://kafka.apache.org/) as an event-bus
+ Standard Extract-Load (EL) capacities for HTTP-POST requests :
	+ from textual data input
	+ from mixed binary/metadata inputs

## Built With
+ FastAPI
+ Aiokafka
+ Aiobotocore

## Setup
### Testing
Tests with : `python -m pytest -o log_cli=true --log-cli-level=INFO`

Testserver works in `localhost` with changes with the embedded `.env`.
For online/on-build testing the `pytest` fixtures are not implemented yet so you'll have to manually change the `.env` variable to be compliant with a local testing setup.  

Build with dockerfile : `docker-compose up --force-recreate --build`

## Usage
Two Routes as an HTTP Endpoint for data ingestion:
+ `/ingress/record-json`: textual data
+ `/ingress/record-json-and-binary`: binary, textual metadatas

Two unsecured routes (data is not checked nor Schema-validated) **only** for debugging purposes:
+ `/ingress/unsecured/record-json`: textual data
+ `/ingress/unsecured/record-json-and-binary`: binary, textual metadatas

# Fast_Clients
## Fast_files
Reuse of ...
If bottlneck and slow transfer, use _s5md_ https://github.com/peak/s5cmd (more complicated, but 12x faster than `boto3` based cli)


### Binary
JSON data shall hold a field named `__resource_path__`

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.


## Roadmap
### Goals for v0.4
+ Better support of exceptions raising for validation
	+ Add testing capabilities for bad validation scenarios
+ clean `InlkSchema`, most of the current properties are duplicates of `confluent_confluent_kafka.schema_registry.schema_registry_client.Schema` (herited from v0.2, where Schema Registry was not used, **Blocking** still waiting for updates in this API).
+ true *async* behaviour, part of the APIs doesn't relies on FastAPI capabilities as they should. **Blocking:** `aiokafka.AIOKafkaAdmin` is still delayed by old release. 
+ Overhaul the behavior of the POST requests, from an *all-in-one* POST request to two POST requests, one with a binary retrieving a job `puuid` in HTTP response 2OO and another from the same client with the metadata and the job `puuid` registered as `__resource_path__` (as a promise). Looser coupling, more opacity, more flexible, faster and safer.
