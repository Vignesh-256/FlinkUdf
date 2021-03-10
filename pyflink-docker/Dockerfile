FROM flink:1.12.0

# install python3 and pip3
RUN apt-get update -y && apt-get install -y python3.7 python3-pip python3.7-dev nano && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install Python Flink
RUN pip3 install apache-flink