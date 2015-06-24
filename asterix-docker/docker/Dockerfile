FROM centos
MAINTAINER Ian Maxon

RUN echo 'LANG="en_US.UTF-8"' > /etc/sysconfig/i18n ;echo 'ZONE="America/Los_Angeles"' > /etc/sysconfig/clock ;cp -a /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN echo "include_only=.us" >> /etc/yum/pluginconf.d/fastestmirror.conf
RUN yum install -y unzip java-1.7.0-openjdk openssh-server openssh-clients python-setuptools wget curl
RUN easy_install supervisor
RUN mkdir /asterixdb
COPY asterix-server*.zip .
RUN unzip asterix-server*.zip -d /asterixdb/
RUN wget -O /asterixdb/bin/asterix-configuration.xml https://gist.githubusercontent.com/parshimers/d6d5fdd18ce9a99568b2/raw/918194395b6d6a4e03b25f796375aee4b73118f0/asterix-configuration.xml
RUN wget -O /etc/supervisord.conf https://gist.githubusercontent.com/parshimers/b1c43a1594909c5d757b/raw/6303f35975c747b13ffb67c7dbb797b57f08e492/gistfile1.txt
RUN wget -O /asterixdb/twu.adm http://asterixdb.googlecode.com/files/twu.adm
RUN wget -O /asterixdb/twm.adm http://asterixdb.googlecode.com/files/twm.adm
RUN wget -O /asterixdb/fbu.adm http://asterixdb.googlecode.com/files/fbu.adm
RUN wget -O /asterixdb/fbm.adm http://asterixdb.googlecode.com/files/fbm.adm

WORKDIR /asterixdb/bin
ENV JAVA_HOME /usr/lib/jvm/jre-1.7.0
ENV JAVA_OPTS -Xmx1536m
EXPOSE 19001 19002 8888 19003 50031

ENTRYPOINT /bin/bash -c '/usr/bin/supervisord'


