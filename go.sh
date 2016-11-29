#!/bin/sh

NAME="iot-mqtt-1"

# -p $MQTT_EXPOSED_PORT:$MQTT_CONTAINER_PORT \

docker run \
 --net=iot-net \
 --name $NAME \
 -d iot/mqtt
