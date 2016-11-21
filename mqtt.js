var mosca = require('mosca')
const mqtt_topic_prefix = 'devices/';

var server;
var ready_cb;
var message_cb;
var client_connected_cb;
var client_disconnected_cb;

var ascoltatore = {
  //using ascoltatore
  type: 'mongo',        
  url: 'mongodb://iot-mongo:27017/mqtt',
  pubsubCollection: 'ascoltatori',
  mongo: {}
};

var moscaSettings = {
  port: 1883,
  backend: ascoltatore,
  persistence: {
    factory: mosca.persistence.Mongo,
    url: 'mongodb://iot-mongo:27017/mqtt'
  }
};

function send(deviceID,json) {
  var topic = mqtt_topic_prefix + deviceID + '/to';
  var payload = JSON.stringify(json);
  console.log('MQTT Send: topic = ',topic,', payload=',payload);
  var message = {
	  topic: topic,
	  payload: payload
  }
  server.publish(message, function() {
	  console.log('MQTT Sent');
  });
}

function set_on_ready(cb) {
	console.log('MQTT Ready CB Registered');
	ready_cb = cb;
}
function set_on_message (cb) {
	message_cb = cb;
}
function set_on_client_connected (cb) {
	client_connected_cb = cb;
}
function set_on_client_disconnected (cb) {
	client_disconnected_cb = cb;
}

function on_message(packet,client) {
	var topic=packet.topic;
	if (topic[0] == '$') { return; }                 // System command
	var msg_str = packet.payload.toString();
	var json = JSON.parse(msg_str);
	if (message_cb) { message_cb(topic,json); }
}

function on_clientConnected(client) {
	console.log('Event: clientConnected');
	if (client_connected_cb) { client_connected_cb(client.id); }
}
function on_clientDisconnected(client) {
	console.log('Event: clientDisconnected');
	if (client_disconnected_cb) { client_disconnected_cb(client.id); }
}

function start() {
	server = new mosca.Server(moscaSettings);
	if (message_cb) {
		console.log('MQTT Message CB Registered');
		server.on('published', on_message);
		server.on('clientConnected', on_clientConnected);
		server.on('clientDisconnected', on_clientDisconnected);
	}
	server.on('ready', function () {
		console.log('Mosca Server Ready');
		if (ready_cb) { ready_cb(); }
	});
}

module.exports.start = start;
module.exports.send = send;
module.exports.set_on_ready = set_on_ready;
module.exports.set_on_message = set_on_message;
module.exports.set_on_client_connected = set_on_client_connected;
module.exports.set_on_client_disconnected = set_on_client_disconnected;
