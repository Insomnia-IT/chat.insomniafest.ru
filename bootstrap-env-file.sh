#!/bin/sh
set -eu

ENV_FILE=".env"
EXAMPLE_FILE=".env.example"

if [ -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE already exists. Refusing to overwrite."
  echo "If you really want to regenerate secrets, delete $ENV_FILE first."
  exit 1
fi

if [ ! -f "$EXAMPLE_FILE" ]; then
  echo "ERROR: $EXAMPLE_FILE not found."
  exit 1
fi

echo "Creating $ENV_FILE from $EXAMPLE_FILE..."

while IFS= read -r line; do
  # Preserve comments and empty lines
  case "$line" in
    ""|\#*)
      echo "$line"
      ;;
    *=)
      key="${line%=}"
      value="$(openssl rand -hex 32)"
      echo "$key=$value"
      ;;
    *)
      echo "$line"
      ;;
  esac
done < "$EXAMPLE_FILE" > "$ENV_FILE"

chmod 600 "$ENV_FILE"

echo "Done."
echo
echo "Generated secrets:"
cut -d= -f1 "$ENV_FILE"
