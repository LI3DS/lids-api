FROM ubuntu:latest

# Install and configure postgresql 9.5
RUN apt-get update -y && \
    apt-get install -y postgresql-9.5 postgresql-server-dev-9.5 \
                       postgresql-client-9.5 postgresql-contrib-9.5 postgis
RUN echo "host   all  all  0.0.0.0/0 md5" >> /etc/postgresql/9.5/main/pg_hba.conf
RUN echo "local  all  all            md5" >> /etc/postgresql/9.5/main/pg_hba.conf
RUN echo "listen_addresses='*'"           >> /etc/postgresql/9.5/main/postgresql.conf

# Install Python 2.7 and Python 3.5
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        python2.7 \
        python2.7-dev \
        python-pip \
        python-setuptools \
        python-numpy \
        python3.5 \
        python3.5-dev \
        python3-pip \
        python3-setuptools \
        python3-numpy \
        postgresql-plpython3-9.5 \
        git \
        make \
        build-essential \
        autoconf \
        automake \
        libxml2-dev \
        graphviz \
    && pip install --upgrade pip \
    && pip3 install --upgrade pip

# Install pg_pointcloud
RUN git clone https://github.com/pgpointcloud/pointcloud.git
RUN cd pointcloud && ./autogen.sh && ./configure && make -j3 && make install

# Install pg_li3ds
RUN git clone https://github.com/li3ds/pg_li3ds
RUN cd pg_li3ds && make install

# Install fdw-li3ds
RUN git clone https://github.com/Kozea/Multicorn && \
    cd Multicorn && \
    PYTHON_OVERRIDE=python2 make -j3 && \
    PYTHON_OVERRIDE=python2 make install && \
    cd .. && \
    git clone https://github.com/LI3DS/fdw-li3ds && \
    cd fdw-li3ds && \
    pip2 install -e .

# Install micmac_li3ds
RUN git clone https://github.com/li3ds/micmac_li3ds.git
RUN cd micmac_li3ds && pip3 install -e .

# Create li3ds user and database
USER postgres
RUN /etc/init.d/postgresql start && \
  psql --command "CREATE USER li3ds WITH SUPERUSER PASSWORD 'li3ds';" && \
  createdb -O li3ds li3ds && \
  psql -d li3ds --command "create extension plpython3u;" && \
  psql -d li3ds --command "create extension postgis;" && \
  psql -d li3ds --command "create extension pointcloud;" && \
  psql -d li3ds --command "create extension pointcloud_postgis;" && \
  psql -d li3ds --command "create extension li3ds;" && \
  psql -d li3ds --command "create extension multicorn;" && \
  psql -d li3ds --command "create server echopulse foreign data wrapper multicorn options ( wrapper 'fdwli3ds.EchoPulse' );" && \
  psql -d li3ds --command "create server sbet foreign data wrapper multicorn options ( wrapper 'fdwli3ds.Sbet' );" && \
  /etc/init.d/postgresql stop
USER root

ENV HOME /webapp
WORKDIR /webapp

# Install Python dependencies
ADD setup.py ./
ADD api_li3ds/__init__.py api_li3ds/
RUN pip3 install -e .[dev,doc]

# Install and configure api-li3ds
ADD conf/api_li3ds.sample.yml conf/api_li3ds.yml
ADD . ./

# Expose postgres and li3ds ports
EXPOSE 5432 5000

CMD ["./docker_api_li3ds.sh"]
