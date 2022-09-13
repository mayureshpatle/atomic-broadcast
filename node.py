from header import *
from broadcast_server import broadcast_server
from application_process import application_process

class Node:
    def __init__(self, node_id):
        """Node Constructor"""

        serv_addr = None
        app_ids = []
        app_addr = {}
        app_msg = {}
        next_addr = None

        config = open("config", "r")
        node_cnt = int(config.readline().strip())
        
        # reading braodcast server details from config file
        servers = {}
        for _ in range(node_cnt):
            id, app_count, ip, port1= config.readline().strip().split()
            id = int(id)
            app_count = int(app_count)

            if id == node_id:       # details are for this node
                node_ip = ip
                serv_port = int(port1)
                serv_addr = (node_ip, serv_port)

                # reading details of application processes of node
                for _ in range(app_count):
                    app_id, app_port, msg = config.readline().strip().split()
                    app_id, app_port = map(int, [app_id, app_port])
                    addr = (node_ip, app_port)
                    app_ids.append(app_id)              # remember the id of this application process
                    app_addr[app_id] = addr             # remember the address of this application process
                    app_msg[app_id] = msg               # remember the messaage file of this application process

            else:                   # details of other node, so ignore application details here
                for _ in range(app_count): config.readline()            
            
            servers[id] = (ip, int(port1))
        
        # reading ring sequence from config file
        ring_seq = list(map(int, config.readline().split()))

        # finding the address of next node
        for i in range(node_cnt):
            if ring_seq[i] == node_id:
                next_node = ring_seq[(i+1)%node_cnt]
                next_addr = servers[next_node]
                break

        # check if this node initially has token
        has_token = int(config.readline()) == node_id
        config.close()

        

        # Creating Processes for server
        server = multiprocessing.Process(target=broadcast_server, args=(node_id, serv_addr, app_addr, next_addr, has_token, ring_seq))

        # Creating Application Processes
        app = {}
        for app_id in app_ids:

            # reading messages from message file of this application process
            messages = []
            msg_file = open(app_msg[app_id], "r")
            for msg in msg_file:
                msg = msg.strip()
                if msg != "":
                    messages.append(msg)
            msg_file.close()
            app[app_id] = multiprocessing.Process(target=application_process, args=(app_id, serv_addr, app_addr[app_id], messages))

        # start broadcast server and wait 1 second
        server.start()
        time.sleep(1)

        # start application process
        for app_id in app_ids:
            app[app_id].start()

        # let the processes keep running
        server.join()
        for i in app_ids:
            app[app_id].join()


if __name__ == "__main__":
    if len(sys.argv)!=2:
        print('Invalid Command Line arguments')
        print("Use the command: python3 node.py node_id")
        print("Here node_id is the id of node as given in config file.")
    else:
        node_id = int(sys.argv[1])
        #set terminal name
        is_windows = hasattr(sys, 'getwindowsversion')
        if is_windows:
            name = f"Node {node_id}"
            system("title "+name)
        else:
            print(f'\33]0;Node {node_id}\a', end='')
            sys.stdout.flush()
        Node(node_id)               # start the node
    