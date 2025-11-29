

docker build -t mydumper-builder-el10 .

docker tag mydumper-builder-el10 mydumper/mydumper-builder-el10:latest
docker tag mydumper-builder-el10 mydumper/mydumper-builder-el10:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-el10
