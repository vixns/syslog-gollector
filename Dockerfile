FROM flynn/busybox
MAINTAINER Emmanuel Gomez <emmanuel@gomez.io>

ADD ./stage/syslog-gollector /bin/syslog-gollector

EXPOSE 514
EXPOSE 514/udp
EXPOSE 8080

ENTRYPOINT ["/bin/syslog-gollector"]
CMD []
