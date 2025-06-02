#!/bin/bash

echo "Starting Cassandra container..."
docker-compose up -d cassandra

echo "Waiting for Cassandra to become healthy..."
until [ "$(docker inspect --format='{{json .State.Health.Status}}' cassandra-container)" == "\"healthy\"" ]; do
  echo "Cassandra is not healthy yet. Retrying in 5 seconds..."
  sleep 5
done

echo "Cassandra is healthy. Proceeding with keyspace and table creation..."

docker exec -i cassandra-container cqlsh <<EOF
CREATE KEYSPACE IF NOT EXISTS final_schema
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE final_schema;

CREATE TABLE edges (
    from_node text,
    to_node text,
    relation text,
    edge_id uuid,
    PRIMARY KEY (from_node, to_node, relation, edge_id)
);

CREATE TABLE node (
    name text,
    label text,
    node_id uuid,
    PRIMARY KEY (name, label)
);

CREATE TABLE edges_bidirectional (
    from_node text,
    to_node text,
    edge_id uuid,
    PRIMARY KEY (from_node, to_node, edge_id)
);
EOF

echo "Keyspace and tables created successfully!"