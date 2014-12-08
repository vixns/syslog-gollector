FROM flynn/busybox
MAINTAINER Emmanuel Gomez <emmanuel@gomez.io>

# syslog
EXPOSE 514
EXPOSE 514/udp
# admin interface
EXPOSE 8080

ADD stage/syslog-gollector /bin/

ENTRYPOINT ["/bin/syslog-gollector"]
CMD []
