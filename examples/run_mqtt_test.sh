#!/bin/bash
# Full stack MQTT test

export MQTT_BROKER_URL="mqtts://mqtt1.anywherescada.joyautomation.com:8883"
export MQTT_USERNAME=sxyxbwk
export MQTT_PASSWORD="7zAe8Dco1obDm27Jar6asSi576apft_Y"
export MQTT_GROUP_ID=Tentacle
export MQTT_EDGE_NODE=EthernetIPTest
export NATS_SERVERS=localhost:4222

echo "=== Starting MQTT Bridge ==="
cd /home/joyja/Development/tentacle-mqtt
timeout 20 deno run --allow-all main.ts &
MQTT_PID=$!

sleep 5

echo ""
echo "=== Running EtherNet/IP Integration Test ==="
cd /home/joyja/Development/tentacle-ethernetip
deno run --allow-net examples/integration_test.ts 2>&1 | grep -v "ethernetip:eip" | head -40

sleep 5
echo ""
echo "=== Waiting for MQTT bridge ==="
wait $MQTT_PID 2>/dev/null
echo "=== Done ==="
