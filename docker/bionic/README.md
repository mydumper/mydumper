

docker build -t mydumper-builder-bionic .

docker tag mydumper-builder-bionic mydumper/mydumper-builder-bionic:latest
docker tag mydumper-builder-bionic mydumper/mydumper-builder-bionic:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-bionic
