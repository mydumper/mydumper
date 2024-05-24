

docker build -t mydumper-builder-nobel .

docker tag mydumper-builder-jammy mydumper/mydumper-builder-nobel:latest
docker tag mydumper-builder-jammy mydumper/mydumper-builder-nobel:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-nobel
