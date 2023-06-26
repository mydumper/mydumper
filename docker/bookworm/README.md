

docker build -t mydumper-builder-bookworm .

docker tag mydumper-builder-bookworm mydumper/mydumper-builder-bookworm:latest
docker tag mydumper-builder-bookworm mydumper/mydumper-builder-bookworm:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-bookworm
