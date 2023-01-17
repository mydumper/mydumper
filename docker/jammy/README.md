

docker build -t mydumper-builder-jammy .

docker tag mydumper-builder-jammy mydumper/mydumper-builder-jammy:latest
docker tag mydumper-builder-jammy mydumper/mydumper-builder-jammy:v0.13.1-2

docker push --all-tags mydumper/mydumper-builder-jammy
