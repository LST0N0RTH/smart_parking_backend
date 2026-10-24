FROM eclipse-mosquitto:2.1.2-alpine
COPY mqtt_broker/start-broker.sh /usr/local/bin/start-broker
RUN chmod 0755 /usr/local/bin/start-broker
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/start-broker"]
