FROM centos
MAINTAINER Ian Maxon

RUN echo 'LANG="en_US.UTF-8"' > /etc/sysconfig/i18n ;echo 'ZONE="America/Los_Angeles"' > /etc/sysconfig/clock ;cp -a /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN echo "include_only=.us" >> /etc/yum/pluginconf.d/fastestmirror.conf
RUN yum install -y unzip java-1.7.0-openjdk openssh-server openssh-clients python-setuptools wget curl
RUN easy_install supervisor
RUN mkdir /asterixdb
COPY asterix-server*.zip .
RUN unzip asterix-server*.zip -d /asterixdb/
COPY asterix-configuration.xml /asterixdb/bin/asterix-configuration.xml
COPY supervisord.conf /etc/supervisord.conf
COPY twu.adm /asterixdb/twu.adm
COPY twm.adm /asterixdb/twm.adm
COPY fbu.adm /asterixdb/fbu.adm
COPY fbm.adm /asterixdb/fbm.adm

WORKDIR /asterixdb/bin
ENV JAVA_HOME /usr/lib/jvm/jre-1.7.0
ENV JAVA_OPTS -Xmx1536m
EXPOSE 19001 19002 8888 19003 50031

ENTRYPOINT /bin/bash -c '/usr/bin/supervisord'


