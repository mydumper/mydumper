

docker build -t mydumper-builder-trixie .

docker tag mydumper-builder-trixie mydumper/mydumper-builder-trixie:latest
docker tag mydumper-builder-trixie mydumper/mydumper-builder-trixie:v0.18.1-1

docker push --all-tags mydumper/mydumper-builder-trixie
