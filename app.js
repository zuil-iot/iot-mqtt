var	mqtt = require('./mqtt'),
	k_in = require('./kafka_consumer'),
	k_out = require('./kafka_producer');

// Topics to subscribe to
var kafka_listen_topics = [
	'to_mqtt'
];
var k_topic = "from_mqtt";


//
// Handle incoming messages
//
//
//From MQTT
function mqtt_to_kafka (topic,json) {
	var topic_fields = topic.split('/');
	if (topic_fields[0] != 'devices') { console.log('[0] = ',topic_fields[0]);return; }	// Wrong mqtt topic
	if (topic_fields[2] != 'from') { console.log('[2] = ',topic_fields[2]);return; }	// Only care about incoming
	console.log('MQTT Rcvd: topic= ', topic, " payload = ",JSON.stringify(json));
	var msg_type=topic_fields[3];
	if (msg_type) {
		var k_msg=json;
		k_out.send(k_topic,k_msg);
	} else {
		console.log('MQTT no message type: ',msg_type);
	}
}

function send_status(deviceID,online) {
	var msg_type="status";
	var k_msg = {
		deviceID: deviceID,
		msg_type: msg_type,
		data: {online: online}
	}
	k_out.send(k_topic,k_msg);
}

function mqtt_client_connected(deviceID) {
	console.log("Connect: ",deviceID);
	send_status(deviceID,true);
}

function mqtt_client_disconnected(deviceID) {
	console.log("Disconnect: ",deviceID);
	send_status(deviceID,false);
}



// From Kafka
function kafka_to_mqtt (topic,json) {
	// Test for valid message here
	mqtt.send(json.deviceID,json);
}

// App is ready
function go () {
	console.log("App Running");
}


// Setup
k_out.set_on_ready(mqtt.start);
mqtt.set_on_ready(k_in.start);
mqtt.set_on_message(mqtt_to_kafka);
mqtt.set_on_client_connected(mqtt_client_connected);
mqtt.set_on_client_disconnected(mqtt_client_disconnected);
k_in.set_on_ready(go);
k_in.set_on_message(kafka_to_mqtt);
k_in.set_topics(kafka_listen_topics);
k_in.set_group('kafka_to_mqtt');
// Go
k_out.start();
