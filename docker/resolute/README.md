

docker build -t mydumper-builder-resolute .

docker tag mydumper-builder-jammy mydumper/mydumper-builder-resolute:latest
docker tag mydumper-builder-jammy mydumper/mydumper-builder-resolute:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-resolute
