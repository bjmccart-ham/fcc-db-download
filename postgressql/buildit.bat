#!/bin/bash

docker build -f ./Dockerfile-ubuntu-pgsql -t ubuntu-postgres:latest  .
docker stop pg
docker rm pg
docker run -d --name pg -p 30432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=My:s3Cr3t/ ubuntu-postgres:latest
docker ps -all
docker logs pg
docker logs pg
docker exec -it pg /bin/bash