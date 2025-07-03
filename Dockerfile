FROM confluentinc/cp-kafka:latest AS mm2-base
COPY --chmod=777 ./mm2_config/mm2.properties /tmp/kafka/config/mm2.properties
CMD ["connect-mirror-maker", "/tmp/kafka/config/mm2.properties"]