FROM ubuntu

RUN apt-get update && \
apt-get install -y cmake git libglib2.0-dev libmysqlclient-dev zlib1g-dev libpcre3-dev libssl-dev && \
git clone https://github.com/davistardif/mydumper.git && \
cd mydumper && \
cmake . && \
make

ENTRYPOINT ["/mydumper/mydumper"]
CMD []