import threading
import queue
import random
import time
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)

class VectorClock:
    def __init__(self, num_nodes, node_id):
        self.clock = [0] * num_nodes
        self.node_id = node_id

    def tick(self):
        self.clock[self.node_id] += 1

    def update(self, other_clock):
        for i in range(len(self.clock)):
            self.clock[i] = max(self.clock[i], other_clock[i])

    def __str__(self):
        return str(self.clock)

class Message:
    def __init__(self, sender, content, timestamp):
        self.sender = sender
        self.content = content
        self.timestamp = timestamp

class Node:
    def __init__(self, node_id, total_nodes, network):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.network = network
        self.clock = VectorClock(total_nodes, node_id)
        self.queue = queue.Queue()
        self.in_cs = False
        self.cs_queue = queue.PriorityQueue()
        self.cs_token = False
        self.replies_needed = 0
        self.replies = set()
        self.parent = None
        self.active = False
        self.lock = threading.Lock()
        self.memory = set()  # Simulate memory management
        self.root_set = set()  # Objects directly accessible

    def send_message(self, recipient_id, message):
        self.clock.tick()
        timestamp = self.clock.clock.copy()
        msg = Message(self.node_id, message, timestamp)
        self.network.send_message(self.node_id, recipient_id, msg)

    def receive_message(self):
        while True:
            sender_id, message = self.queue.get()
            self.clock.update(message.timestamp)
            self.handle_message(sender_id, message)

    def handle_message(self, sender_id, message):
        if message.content['type'] == 'cs_request':
            if self.in_cs or (self.cs_token and not self.cs_queue.empty()):
                self.cs_queue.put((message.timestamp, sender_id))
            else:
                self.cs_token = False
                self.send_message(sender_id, {'type': 'cs_token'})

        elif message.content['type'] == 'cs_token':
            self.cs_token = True
            if not self.cs_queue.empty():
                _, req_sender_id = self.cs_queue.get()
                self.in_cs = True
                self.execute_critical_section(req_sender_id)

        elif message.content['type'] == 'termination':
            if sender_id == self.parent:
                self.replies_needed -= 1
                if self.replies_needed == 0:
                    if self.active:
                        self.parent = None
                        self.active = False
                        self.send_message(self.parent, {'type': 'termination'})
                    else:
                        self.parent = None
                        self.active = False

    def request_cs(self):
        self.clock.tick()
        timestamp = self.clock.clock.copy()
        self.cs_queue.put((timestamp, self.node_id))
        for i in range(self.total_nodes):
            if i != self.node_id:
                self.send_message(i, {'type': 'cs_request'})

        while not self.cs_token:
            time.sleep(0.1)

        self.in_cs = True
        self.execute_critical_section(self.node_id)
        self.cs_token = False
        if not self.cs_queue.empty():
            _, req_sender_id = self.cs_queue.get()
            self.send_message(req_sender_id, {'type': 'cs_token'})

    def execute_critical_section(self, sender_id):
        logging.info(f"Node {self.node_id} is entering critical section.")
        time.sleep(random.uniform(0.1, 0.5))
        logging.info(f"Node {self.node_id} is leaving critical section.")
        self.in_cs = False
        self.send_message(sender_id, {'type': 'cs_token'})

    def perform_garbage_collection(self):
        with self.lock:
            logging.info(f"Node {self.node_id} performing garbage collection.")
            # Cheney's algorithm simulation
            self.memory = self.root_set.copy()  # Assume all accessible memory is the root set
            time.sleep(random.uniform(0.1, 0.5))
            logging.info(f"Node {self.node_id} completed garbage collection.")

    def start_termination_detection(self):
        self.parent = self.node_id
        self.replies_needed = self.total_nodes - 1
        self.active = True
        for i in range(self.total_nodes):
            if i != self.node_id:
                self.send_message(i, {'type': 'termination'})

    def handle_termination(self):
        if self.replies_needed == 0:
            if self.active:
                self.parent = None
                self.active = False
                self.send_message(self.parent, {'type': 'termination'})
            else:
                self.parent = None
                self.active = False

class Network:
    def __init__(self, total_nodes):
        self.total_nodes = total_nodes
        self.nodes = [Node(i, total_nodes, self) for i in range(total_nodes)]
        self.message_queues = [queue.Queue() for _ in range(total_nodes)]
        self.threads = []

    def send_message(self, sender_id, recipient_id, message):
        self.message_queues[recipient_id].put((sender_id, message))

    def start(self):
        for node in self.nodes:
            thread = threading.Thread(target=node.receive_message)
            thread.daemon = True
            thread.start()
            self.threads.append(thread)

    def stop(self):
        for thread in self.threads:
            thread.join()

# Simulación de la ejecución
total_nodes = 5
network = Network(total_nodes)
network.start()

# Sincronización de relojes y ejecución de tareas científicas
for node in network.nodes:
    thread = threading.Thread(target=node.request_cs)
    thread.start()

# Iniciar detección de terminación
network.nodes[0].start_termination_detection()

# Realizar recolección de basura en los nodos
for node in network.nodes:
    node.perform_garbage_collection()

# Esperar a que las operaciones se completen
time.sleep(5)

network.stop()

