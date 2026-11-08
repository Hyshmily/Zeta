# Zeta Worker — the cluster's decision plane. The only runtime dependency is
# RabbitMQ (App reports in, HOT/COOL decisions and heartbeats out).
#
# Build prerequisite — this Dockerfile copies the packaged JAR, so package first:
#   mvn clean package -pl worker -am
#   docker build -t zeta-worker worker/
FROM eclipse-temurin:17-jre

# curl backs the HEALTHCHECK below. Install it explicitly: the temurin JRE
# image does not guarantee curl in its runtime package set.
RUN apt-get update \
  && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/* \
  && groupadd --gid 10001 zeta \
  && useradd --uid 10001 --gid zeta --no-create-home --shell /usr/sbin/nologin zeta

WORKDIR /app
COPY --chown=zeta:zeta target/zeta-worker-*.jar app.jar

# Epoch storage: the Worker raises its epoch on every start and Apps reject
# decisions from an incarnation that does not raise it. Mount a volume at
# /var/lib/zeta (docker-compose.yml maps worker_epoch here); a fresh named
# volume inherits this directory's ownership from the image.
RUN mkdir -p /var/lib/zeta && chown zeta:zeta /var/lib/zeta
ENV ZETA_WORKER_EPOCH_FILE=/var/lib/zeta/epoch

USER zeta

EXPOSE 8080

# Operational escape hatch (e.g. JAVA_OPTS=-Xmx512m). Unset by default: the JVM
# sizes its heap from the container limit on its own (UseContainerSupport).
ENV JAVA_OPTS=""

# Probes are split by dependency: liveness must not include AMQP, or a broker
# outage would restart a healthy Worker (losing its in-memory windows and
# bumping its epoch); readiness includes RabbitMQ so the instance is taken out
# of service while reports cannot be consumed.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS http://localhost:8080/actuator/health/liveness || exit 1

# exec keeps java as PID 1 so SIGTERM reaches the JVM for a graceful shutdown.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
