

docker build -t mydumper-builder-el9 .

docker tag mydumper-builder-el9 mydumper/mydumper-builder-el9:latest
docker tag mydumper-builder-el9 mydumper/mydumper-builder-el9:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-el9
