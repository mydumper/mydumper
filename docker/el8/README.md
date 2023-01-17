

docker build -t mydumper-builder-el8 .

docker tag mydumper-builder-el8 mydumper/mydumper-builder-el8:latest
docker tag mydumper-builder-el8 mydumper/mydumper-builder-el8:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-el8
