from header import *

class application_process:
    def __init__(self,app_id, serv_addr, app_addr, messages):
        """Application Process"""
        self.app = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.app.bind(app_addr)
        self.app_id = app_id
        self.serv_addr = serv_addr
        makedirs("./output", 0o777, True)
        self.output = f"./output/app_{app_id}"

        # create an empty file for outputs of this application process
        output = open(self.output, "w")
        output.close() 

        # create a thread to receive messages from braodcast server
        recv_thread = threading.Thread(target=self.recv_message_from_server)
        recv_thread.start()

        line = "="*50

        print(f"\n{line}\nApplication Process {app_id} Started\n(All further ouputs of this process are redirected to ./output/app_{app_id} file)")

        for msg in messages:
            threading.Thread(target=self.broadcast_message, args=(msg,)).start()

        # keep receiving messages fron broadcast server
        recv_thread.join()


    def recv_message_from_server(self):
        """receives message from broadcast server of this node"""
        last_msg_no = -1
        while True:
            msg, addr = self.app.recvfrom(65535)
            msg = pickle.loads(msg)
            if msg.seq_no == last_msg_no + 1:
                last_msg_no += 1
                display_msg = msg.details()         # display only if message is not received earlier

                # redirect output to file
                output = open(self.output, "a")
                output.write(display_msg)
                output.close()
                
            if msg.seq_no <= last_msg_no:                       
                ack = appAcknowledgement(msg.seq_no, self.app_id)
                self.app.sendto(pickle.dumps(ack), self.serv_addr)  # acknowledge receipt of message


    def broadcast_message(self,msg):
        """send a message for bradcasting to bradcast server of this node"""
        msg = (msg, self.app_id)
        self.app.sendto(pickle.dumps(msg), self.serv_addr)