#!/bin/bash
# Start the Bosonic-only client
cd "$(dirname "$0")"
python3 -m src.websockets.bosonic_only_client
