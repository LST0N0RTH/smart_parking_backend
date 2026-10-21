#!/bin/sh
set -eu

broker_port="${PORT:-8080}"
mqtt_username="${MQTT_USERNAME:-}"
mqtt_password="${MQTT_PASSWORD:-}"

case "$broker_port" in
  ''|*[!0-9]*)
    echo "PORT must be a number" >&2
    exit 1
    ;;
esac

if [ "$broker_port" -lt 1 ] || [ "$broker_port" -gt 65535 ]; then
  echo "PORT must be between 1 and 65535" >&2
  exit 1
fi

if [ -z "$mqtt_username" ] || [ -z "$mqtt_password" ]; then
  echo "MQTT_USERNAME and MQTT_PASSWORD must both be configured" >&2
  exit 1
fi

case "$mqtt_username" in
  *:*)
    echo "MQTT_USERNAME must not contain a colon" >&2
    exit 1
    ;;
esac

config_dir="/mosquitto/config"
data_dir="/mosquitto/data"
log_dir="/mosquitto/log"
password_file="$config_dir/passwords"
config_file="$config_dir/mosquitto.conf"

install -d -m 0750 "$config_dir" "$data_dir" "$log_dir"
umask 077
mosquitto_passwd -b -c "$password_file" "$mqtt_username" "$mqtt_password"

cat >"$config_file" <<EOF
listener $broker_port 0.0.0.0
protocol websockets

allow_anonymous false
password_file $password_file

persistence true
persistence_location $data_dir/
autosave_interval 60
autosave_on_changes true

log_dest stdout
connection_messages true
log_timestamp true
EOF

chown -R mosquitto:mosquitto "$config_dir" "$data_dir" "$log_dir"
chmod 0600 "$password_file"

exec /usr/sbin/mosquitto -c "$config_file"