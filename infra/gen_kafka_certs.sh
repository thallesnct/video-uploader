#!/usr/bin/env bash
# Generates the self-signed keystore/truststore for the Kafka prod-profile
# rehearsal (infra/kafka_prod_verify.py, `make kafka-prod-verify`). Output
# goes to infra/kafka-certs/ — gitignored, regenerated fresh each run, never
# committed (these are rehearsal-only secrets with a hardcoded password,
# not real credentials).
#
# One shared self-signed cert for all three brokers, not a proper per-broker
# CA chain: this proves the SASL_SSL listener config and client
# authentication work end to end, which is the phase's actual gate — a real
# deployment would issue per-broker certs from a real CA, out of scope for a
# rehearsal this project tears down immediately after (PLAN.md's scope
# note: no live deployment target). Inter-broker traffic stays on the
# PLAINTEXT listener (KAFKA_INTER_BROKER_LISTENER_NAME), so brokers never
# TLS-handshake with each other — only the client-facing SASL_SSL listener
# needs a server cert, so one shared keystore is enough.
#
# Runs keytool inside the same cp-kafka image the brokers use (it bundles a
# JDK) rather than requiring keytool on the host — matching AGENTS.md's
# "ffmpeg only in worker images" reasoning applied to a different tool: this
# stays a Docker-only dependency, not a new host requirement.
set -euo pipefail
cd "$(dirname "$0")/.."

CERTS_DIR="$PWD/infra/kafka-certs"
STOREPASS="rehearsal-only-not-a-real-secret"

rm -rf "$CERTS_DIR"
mkdir -p "$CERTS_DIR"

docker run --rm -v "$CERTS_DIR:/certs" -w /certs confluentinc/cp-kafka:7.6.1 bash -c "
set -euo pipefail
keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 -validity 3650 \
  -keystore kafka.keystore.jks -storepass '$STOREPASS' -keypass '$STOREPASS' \
  -dname 'CN=kafka, OU=video-pipeline, O=rehearsal, L=local, ST=NA, C=US' \
  -ext 'SAN=dns:kafka-1,dns:kafka-2,dns:kafka-3,dns:localhost'

keytool -exportcert -alias kafka -keystore kafka.keystore.jks -storepass '$STOREPASS' \
  -rfc -file kafka.cert.pem

keytool -importcert -alias kafka -keystore kafka.truststore.jks -storepass '$STOREPASS' \
  -file kafka.cert.pem -noprompt

for f in keystore_creds key_creds truststore_creds; do
  printf '%s' '$STOREPASS' > \"\$f\"
done
"

cat > "$CERTS_DIR/kafka_server_jaas.conf" <<JAAS
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="pipeline"
  password="pipeline-secret"
  user_pipeline="pipeline-secret";
};
JAAS

cat > "$CERTS_DIR/client.properties" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="pipeline" password="pipeline-secret";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password=$STOREPASS
ssl.endpoint.identification.algorithm=
PROPS

echo "generated $CERTS_DIR: kafka.keystore.jks, kafka.truststore.jks, kafka_server_jaas.conf, client.properties"
