import threading
import time
import random
import queue

class Message:
    def __init__(self, sender, content, timestamp):
        self.sender = sender
        self.content = content
        self.timestamp = timestamp

class Node:
    def __init__(self, node_id, network):
        self.node_id = node_id
        self.network = network
        self.data_store = {}
        self.log = []
        self.current_term = 0
        self.voted_for = None
        self.commit_index = 0
        self.state = 'follower'
        self.votes_received = 0
        self.lock = threading.Lock()
        self.message_queue = queue.Queue()
        self.alive = True

    def send_message(self, recipient_id, message):
        self.network.send_message(self.node_id, recipient_id, message)

    def receive_message(self, message):
        self.message_queue.put(message)

    def handle_message(self, message):
        if message.content['type'] == 'request_vote':
            self.handle_request_vote(message)
        elif message.content['type'] == 'append_entries':
            self.handle_append_entries(message)
        elif message.content['type'] == 'vote':
            self.handle_vote(message)
        elif message.content['type'] == 'ack':
            self.handle_ack(message)

    def handle_request_vote(self, message):
        with self.lock:
            if (message.content['term'] > self.current_term and 
                (self.voted_for is None or self.voted_for == message.sender)):
                self.voted_for = message.sender
                self.current_term = message.content['term']
                response = Message(self.node_id, {'type': 'vote', 'term': self.current_term}, time.time())
                self.send_message(message.sender, response)

    def handle_append_entries(self, message):
        with self.lock:
            if message.content['term'] >= self.current_term:
                self.state = 'follower'
                self.current_term = message.content['term']
                self.log.append(message.content['entry'])
                self.commit_index = message.content['commit_index']
                response = Message(self.node_id, {'type': 'ack', 'term': self.current_term}, time.time())
                self.send_message(message.sender, response)

    def handle_vote(self, message):
        with self.lock:
            if message.content['term'] == self.current_term:
                self.votes_received += 1

    def handle_ack(self, message):
        pass

    def request_votes(self):
        with self.lock:
            self.current_term += 1
            self.votes_received = 1
            self.voted_for = self.node_id
        for node in self.network.nodes:
            if node.node_id != self.node_id and node.alive:
                message = Message(self.node_id, {'type': 'request_vote', 'term': self.current_term}, time.time())
                self.send_message(node.node_id, message)

    def append_entries(self, entry):
        with self.lock:
            self.log.append(entry)
            self.commit_index += 1
        for node in self.network.nodes:
            if node.node_id != self.node_id and node.alive:
                message = Message(self.node_id, {'type': 'append_entries', 'term': self.current_term, 'entry': entry, 'commit_index': self.commit_index}, time.time())
                self.send_message(node.node_id, message)

    def run_node(self):
        while self.alive:
            try:
                message = self.message_queue.get(timeout=1)
                self.handle_message(message)
            except queue.Empty:
                pass

            if self.state == 'follower':
                time.sleep(random.uniform(1, 3))
                if self.state == 'follower':
                    self.state = 'candidate'
            elif self.state == 'candidate':
                self.request_votes()
                time.sleep(random.uniform(1, 3))
                if self.votes_received > len(self.network.nodes) // 2:
                    self.state = 'leader'
            elif self.state == 'leader':
                self.append_entries({'data': 'some_data'})
                time.sleep(1)

    def simulate_failure(self):
        self.alive = False
        print(f"Node {self.node_id} failed.")

    def recover(self):
        self.alive = True
        self.state = 'follower'
        print(f"Node {self.node_id} recovered.")

class Network:
    def __init__(self, total_nodes):
        self.nodes = [Node(i, self) for i in range(total_nodes)]
        self.latency = 0.5  # Simulación de latencia en la red

    def send_message(self, sender_id, recipient_id, message):
        if self.nodes[recipient_id].alive:
            time.sleep(self.latency)  # Simular latencia de red
            self.nodes[recipient_id].receive_message(message)

    def start_network(self):
        threads = [threading.Thread(target=node.run_node) for node in self.nodes]
        for thread in threads:
            thread.start()
        return threads

    def simulate_partition(self):
        partitioned_node = random.choice(self.nodes)
        partitioned_node.simulate_failure()
        time.sleep(random.uniform(5, 10))
        partitioned_node.recover()

def simulate():
    total_nodes = 5
    network = Network(total_nodes)
    threads = network.start_network()

    # Simular fallos aleatorios y particiones de red
    for _ in range(3):
        time.sleep(random.uniform(10, 20))
        network.simulate_partition()

    for thread in threads:
        thread.join()

simulate()

