

docker build -t mydumper-builder-buster .

docker tag mydumper-builder-buster mydumper/mydumper-builder-buster:latest
docker tag mydumper-builder-buster mydumper/mydumper-builder-buster:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-buster
