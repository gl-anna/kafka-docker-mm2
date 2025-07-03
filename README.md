# kafka-docker-mm2

Example of how to setup Kafka Docker multi cluster environment with MirrorMaker2

Read
the [Medium article](https://medium.com/larus-team/how-to-setup-mirrormaker-2-0-on-apache-kafka-multi-cluster-environment-87712d7997a4)
and simply follow the provided step by step guide on how to use this repo

## Usage

spin up docker:

```bash
docker-compose up -d
```

## Node tests

NOTE: make sure you have Node `v18.7.0` or later installed on your machine; check `node -v`

```bash
cd node-test

npm i
```

create topics and generate messages

```bash
npx ts-node src/create-topics-and-msgs.ts
```

run tests

```bash
npx ts-node src/test-mirrormaker.ts
```

## Java tests

connect to broker 1A

```bash
docker exec -it broker1A /bin/bash
```

create topic:

```bash
kafka-topics --bootstrap-server broker1A:29092 --create --topic topic1 --partitions 3 --replication-factor 3 
```

get topics at broker 1A:

```bash
kafka-topics --bootstrap-server broker1A:29092 --list
```

connect to broker 1B

```bash
docker exec -it broker1B /bin/bash
```

create topic:

```bash
kafka-topics --bootstrap-server broker1B:29093 --create --topic topic1 --partitions 3 --replication-factor 3 
```

get topics at broker 1B:

```bash
kafka-topics --bootstrap-server broker1B:29093 --list
```

```bash
cd testOffsetMm2
java -jar target/TestOffsetMm2-1.0-SNAPSHOT.jar
```

