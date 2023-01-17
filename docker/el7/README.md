

docker build -t mydumper-builder-el7 .

docker tag mydumper-builder-el7 mydumper/mydumper-builder-el7:latest
docker tag mydumper-builder-el7 mydumper/mydumper-builder-el7:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-el7
