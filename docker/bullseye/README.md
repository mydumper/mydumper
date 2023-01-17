

docker build -t mydumper-builder-bullseye .

docker tag mydumper-builder-bullseye mydumper/mydumper-builder-bullseye:latest
docker tag mydumper-builder-bullseye mydumper/mydumper-builder-bullseye:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-bullseye
