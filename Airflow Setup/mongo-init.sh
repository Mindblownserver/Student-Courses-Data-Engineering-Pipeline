#!/bin/bash
set -euo pipefail # run in strict mode

# Wait for MongoDB to be ready
echo "Waiting for MongoDB to start..."
until mongosh --host localhost:27017 \
  --username ${MONGO_INITDB_ROOT_USERNAME} \
  --password ${MONGO_INITDB_ROOT_PASSWORD} \
  --authenticationDatabase admin \
  --eval "print('MongoDB is ready')" > /dev/null 2>&1
do
  echo "Waiting for MongoDB connection..."
  sleep 2
done

echo "MongoDB is ready. Starting data import..."

# Define database name
DATABASE="mydb"

# Import users.json
echo "Importing users collection..."
mongoimport --host localhost:27017 \
  --username ${MONGO_INITDB_ROOT_USERNAME} \
  --password ${MONGO_INITDB_ROOT_PASSWORD} \
  --authenticationDatabase admin \
  --db ${DATABASE} \
  --collection users \
  --type json \
  --file /tmp/data/users.json \
  --jsonArray \
  --drop  # --drop removes existing collection before import

# Import students.json
echo "Importing students collection..."
mongoimport --host localhost:27017 \
  --username ${MONGO_INITDB_ROOT_USERNAME} \
  --password ${MONGO_INITDB_ROOT_PASSWORD} \
  --authenticationDatabase admin \
  --db ${DATABASE} \
  --collection students \
  --type json \
  --file /tmp/data/students.json \
  --jsonArray \
  --drop

# Import courses.json
echo "Importing courses collection..."
mongoimport --host localhost:27017 \
  --username ${MONGO_INITDB_ROOT_USERNAME} \
  --password ${MONGO_INITDB_ROOT_PASSWORD} \
  --authenticationDatabase admin \
  --db ${DATABASE} \
  --collection courses \
  --type json \
  --file /tmp/data/courses.json \
  --jsonArray \
  --drop

echo "Data import completed. Creating read-only user..."


echo "========================================="
echo "Initialization complete!"
echo "Database: ${DATABASE}"
echo "Collections imported: users, students, courses"
echo "========================================="