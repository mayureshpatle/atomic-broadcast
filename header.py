"""
This file contains definitions of classes and includes python packages required for implementation
"""

import sys
import time
from header import *
import multiprocessing
import threading
import socket
import pickle
import queue
from os import system, makedirs

class message_body:
    def __init__(self, seq_no, sender_app, msg):
        self.seq_no = seq_no
        self.sender_app = sender_app
        self.msg = msg

    def details(self):
        return f"\t(Sequence No: {self.seq_no} | Message: {self.msg} | Sender Application: {self.sender_app})\n"

class message_info:
    def __init__(self, seq_no, message_content, sender_node, nodes):
        """message_info constructor"""
        self.seq_no = seq_no
        self.body = message_content[0]
        self.sender_app = message_content[1]
        self.sender_node = sender_node
        self.stability_time = None
        self.is_stable = 0
        self.ack = {}               # will store the acknowledgement status as a mapping (node_no -> ack)
        for node in nodes:
            self.ack[node] = False

    def read(self, node_no):
        """return message details, and acknowledges the message for node_no"""
        self.ack[node_no] = True
        return message_body(self.seq_no, self.sender_app, self.body)

    def check_stability(self):
        """Returns 0 if message is not stable, 1 if message is already stable, 2 if this message is just now stabalized"""
        if self.is_stable:
            return 2
        for node in self.ack:
            if not self.ack[node]:
                return 0
        self.is_stable = 1
        return 1

    def set_stability_time(self, stability_time):
        """Sets the stability time of message, after completing 2 rounds"""
        self.stability_time = stability_time

class Token:
    def __init__(self, node_cnt):
        """token constructor"""
        self.sequence_no = 0        # sequence number will be auto incremented after consumption by any broadcast server
        self.bdc_messages = []
        self.stable_cnt = 0 
        self.node_cnt = node_cnt          

    def get_seq_no(self):
        """returns global sequence number to be assigned for a message"""
        seq_no = self.sequence_no
        self.sequence_no += 1
        return seq_no

    def append_msg(self, msg):
        """adds msg to the list of messages to be broadcasted"""
        self.bdc_messages.append(msg)

    def message_stabalized(self):
        self.stable_cnt += 1

    def get_stability_details(self):
        """returns the details of stability time of messages, returns none if all messages were not circulated 2 times"""
        stable_time = []
        total_time = 0
        for msg in self.bdc_messages:
            if type(msg.stability_time) == type(None):
                return None 
            stable_time.append(f"Msg No:{msg.seq_no} -> {msg.stability_time} seconds")
            total_time += msg.stability_time
        avg_time = total_time / self.sequence_no
        stable_time.append(f"Average Stability Time: {avg_time} seconds")
        return stable_time
    
class appAcknowledgement:
    def __init__(self, seq_no, app_id):
        self.seq_no = seq_no
        self.app_id = app_id
        