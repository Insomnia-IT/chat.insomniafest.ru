#!/bin/sh
set -eu

# Verify required env vars exist
: "${LIVEKIT_API_KEY:?missing}"
: "${LIVEKIT_API_SECRET:?missing}"

# Render livekit.yaml from template
sed \
  -e "s|\${LIVEKIT_API_KEY}|${LIVEKIT_API_KEY}|g" \
  -e "s|\${LIVEKIT_API_SECRET}|${LIVEKIT_API_SECRET}|g" \
  /etc/livekit.yaml.tmpl > /etc/livekit.yaml

exec /livekit-server --config /etc/livekit.yaml
