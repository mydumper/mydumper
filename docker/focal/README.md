

docker build -t mydumper-builder-focal .

docker tag mydumper-builder-focal mydumper/mydumper-builder-focal:latest
docker tag mydumper-builder-focal mydumper/mydumper-builder-focal:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-focal
