#!/bin/bash
# Start the Bosonic Trad5 client
cd "$(dirname "$0")"
python3 -m src.websockets.bosonic_trad5_client
