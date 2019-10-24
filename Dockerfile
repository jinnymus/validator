FROM openjdk:11
COPY ./time-to-start.sh /app/time-to-start.sh
COPY ./target /app/

EXPOSE 8080

ENTRYPOINT ["bash", "/app/time-to-start.sh"]
