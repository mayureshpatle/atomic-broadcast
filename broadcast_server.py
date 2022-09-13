from header import *

class broadcast_server:
    def __init__(self, node_id, serv_addr, app_addr, next_addr, has_token_initially, ring_seq):
        """Broadcast Server Process"""

        self.node_id = node_id
        self.ring_seq = ring_seq
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.bind(serv_addr)
        self.app_addr = app_addr
        self.next_addr = next_addr
        self.has_token_initially = has_token_initially

        self.app_msg_queue = queue.Queue()
        self.received_messages = {}                                 # mapping from seq_no -> (msg, received_time)
        self.app_ack = {}
        for app in app_addr:
            self.app_ack[app] = {}
        
        self.my_seen_count = {}
        self.stability_timestamp = {}

        receiver = threading.Thread(target=self.receive_messages)
        receiver.start()

        sender = {}
        for app in app_addr:
            sender[app] = threading.Thread(target=self.send_to_application, args=(app,))
            sender[app].start()

        print("Broadcast Server Started at node", node_id)
        
        if has_token_initially:
            time.sleep(1)
            # print("Ensure that all nodes are started within 2 minutes of starting the first node")
            # print("Waiting for 2 minutes to start execution")
            # time.sleep(10)
            token = Token(len(ring_seq))
            threading.Thread(target=self.process_token, args=(token,)).start()

        # let the receiver and sender threads keep running
        receiver.join()
        for app in app_addr: sender[app].join()

    def receive_messages(self):
        """receives messages and performs actions accordingly"""
        sample_ack = appAcknowledgement(-1,-1)
        sample_token = Token(0)
        while True:
            msg, addr = self.s.recvfrom(65535)
            msg = pickle.loads(msg)

            # broadcast request/acknowledgement received from application process
            if type(msg) == type(sample_ack):                   # acknowledgement_received
                self.app_ack[msg.app_id][msg.seq_no] = True
            elif type(msg) == type(("",0)):                     # braodcast message received
                self.app_msg_queue.put(msg)

            # token received
            elif type(msg) == type(sample_token):
                threading.Thread(target=self.process_token, args=(msg,)).start()

    def process_token(self, token):
        """processes a token and forwards it to next node"""
        # append all messages from application to the  token
        while not self.app_msg_queue.empty():
            seq_no = token.get_seq_no()
            msg = self.app_msg_queue.get()
            message = message_info(seq_no, msg, self.node_id, self.ring_seq)
            token.append_msg(message)

        # read available messages from token
        for message in token.bdc_messages:
            seq_no = message.seq_no

            # stability of this message was already confirmed at this node 
            if seq_no in self.stability_timestamp:
                pass
            else:
                # check if this message is seen first time by this node
                if seq_no not in self.received_messages:
                    self.received_messages[seq_no] = (message.read(self.node_id), time.time())
                
                # check stability of message
                stability_status = message.check_stability()
                if stability_status>0:
                    self.stability_timestamp[seq_no] = time.time()
                    if stability_status == 1: token.message_stabalized()
                
            if seq_no not in self.my_seen_count:
                self.my_seen_count[seq_no] = 0
            else:
                self.my_seen_count[seq_no] += 1
                if message.sender_node == self.node_id and self.my_seen_count[seq_no] == 2:
                    message.set_stability_time(time.time() - self.received_messages[seq_no][1])

        # print stability time details after all messages that are diplayed
        if self.has_token_initially and len(self.stability_timestamp) and len(self.stability_timestamp)==token.sequence_no:
            stability_details = token.get_stability_details()
            if type(stability_details) != type(None):
                print("\n"+"="*50)
                print("Details of Stability Time:")
                print(*stability_details, sep = "\n")
                self.has_token_initially = False

        # send token to broadcast server of next node
        self.s.sendto(pickle.dumps(token), self.next_addr)

    def send_to_application(self, app_id):
        """sends stable messages to application process in sequence"""
        next_seq_no = 0
        while True:
            if next_seq_no in self.app_ack[app_id]: # go to next message if this message was delivered
                next_seq_no += 1
            if next_seq_no in self.stability_timestamp:  # unsent/unacknowledged message
                self.s.sendto(pickle.dumps(self.received_messages[next_seq_no][0]), self.app_addr[app_id])
                time.sleep(1)
