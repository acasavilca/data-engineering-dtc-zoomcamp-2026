#!/bin/bash
while IFS='=' read -r key value; do
    # Skip empty lines
    [ -z "$key" ] && continue

    # Encode the value part
    encoded_value=$(printf "%s" "$value" | base64 -w 0)

    # Print result
    echo "SECRET_$key=$encoded_value"
done < "$1" > "${1}_encoded"
