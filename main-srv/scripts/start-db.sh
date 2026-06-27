#!/bin/bash
set -e

cd /mnt/work/opora/main-srv/configs

echo "Starting PostgreSQL and Qdrant in Docker..."
docker compose up -d

echo "Waiting for services to be ready..."

# Wait for PostgreSQL
echo -n "  PostgreSQL: "
until docker exec postgres_opora_db pg_isready -U postgres &>/dev/null; do
    echo -n "."
    sleep 1
done
echo " ready"

# Wait for Qdrant (check HTTP endpoint)
echo -n "  Qdrant: "
until curl -s -f http://localhost:6335/healthz &>/dev/null; do
    echo -n "."
    sleep 1
done
echo " ready"

echo "Status check:"
docker ps --filter name=postgres_opora_db --format "table {{.Names}}\t{{.Status}}"
docker ps --filter name=qdrant_opora_db --format "table {{.Names}}\t{{.Status}}"

echo "Logs (last 10 lines):"
docker logs --tail 10 postgres_opora_db
docker logs --tail 10 qdrant_opora_db