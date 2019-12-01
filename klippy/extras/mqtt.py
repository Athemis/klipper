# Support for Mqtt messages
#
# Copyright (C) 2019  Alexander Minges <alexander.minges@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license.

import uuid
import paho.mqtt.client as mqtt


class PrinterMqtt(object):
    def __init__(self, config):
        self.printer = config.get_printer()
        self.protocols = {'mqttv31': 0, 'mqttv311': 1, 'mqttv5': 2}

        # set up mqtt broker connection
        broker_url = config.get('broker_url', '127.0.0.1')
        broker_port = config.getint('broker_port', 1883)
        broker_keepalive = config.getint('broker_keepalive', 60)
        client_id = config.get('client_id', str(uuid.uuid4())) # use random UUID by default as client ID
        protocol_version = config.getchoice('protocol', self.protocols, 'mqttv31')
        self.topic_base = config.get('topic', 'klipper') # base topic to publish messages

        if protocol_version == 2:
            protocol = mqtt.MQTTv5
        elif protocol_version == 1:
            protocol = mqtt.MQTTv311
        elif protocol_version == 0:
            protocol = mqtt.MQTTv31
        else:
            raise config.error("Protocol version has to be one of 'mqttv31', 'mqttv311' or 'mqttv5'")

        self.client = mqtt.Client(client_id, protocol)
        self.client.connect(broker_url, port=broker_port, keepalive=broker_keepalive)
        self.client.loop_start()

        self.printer.register_event_handler('klippy:connect', self.handle_connect)
        self.printer.register_event_handler('klippy:disconnect', self.handle_disconnect)
        self.printer.register_event_handler('klippy:shutdown', self.handle_shutdown)
        self.printer.register_event_handler('klippy:ready', self.handle_ready)
    
    def handle_connect(self):
        reactor = self.printer.get_reactor()
        # connect to broker and start publishing messages
        self.check_timer = reactor.register_timer(self.update, reactor.NOW)

        messages = [{'topic': 'klippy/status', 'payload': 'connecting'}]
        self.publish(messages)

    def handle_disconnect(self):
        messages = [{'topic': 'klippy/status', 'payload': 'disconnected'}]
        self.publish(messages)

        self.client.loop_stop()
        self.client.disconnect()

        reactor = self.printer.get_reactor()
        # kill connection to broker and stop publishing updates
        reactor.update_timer(self.update, reactor.NEVER)

    def handle_shutdown(self):
        messages = [{'topic': 'klippy/status', 'payload': 'shutdown'}]
        self.publish(messages)

    def handle_ready(self):
        messages = [{'topic': 'klippy/status', 'payload': 'ready'}]
        self.publish(messages)

    def update(self, eventtime):
        

    def publish(self, messages):
    # Publish messages to mqtt broker. Accepts list of dictionaries with keys 'topic' and 'payload'
        for message in messages:
            self.client.publish('{}/{}'.format(self.topic_base, message['topic']), message['payload'])

def load_config(config):
    return PrinterMqtt(config)
