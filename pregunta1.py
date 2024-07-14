import asyncio
import logging
import threading
import time
from queue import PriorityQueue

# Configuración del registro de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

class Event:
    def __init__(self, priority, event_type, data, timestamp):
        self.priority = priority
        self.event_type = event_type
        self.data = data
        self.timestamp = timestamp

    def __lt__(self, other):
        return self.priority < other.priority

class Notebook:
    def __init__(self):
        self.cells = []
        self.event_queue = PriorityQueue()
        self.lock = threading.Lock()

    async def execute_cell(self, cell_id):
        with self.lock:
            try:
                logging.info(f"Executing cell {cell_id}")
                await asyncio.sleep(2)  # Simulación de ejecución
                self.cells[cell_id]['output'] = f"Output of cell {cell_id}"
                logging.info(f"Cell {cell_id} executed successfully")
            except Exception as e:
                logging.error(f"Error executing cell {cell_id}: {str(e)}")

    def add_cell(self, content):
        with self.lock:
            cell_id = len(self.cells)
            self.cells.append({'id': cell_id, 'content': content, 'output': None})
            logging.info(f"Cell {cell_id} added with content: {content}")

    async def handle_event(self, event):
        with self.lock:
            logging.info(f"Handling event: {event.event_type} with data: {event.data}")
            if event.event_type == 'execute_cell':
                await self.execute_cell(event.data)

    async def run_event_loop(self):
        while True:
            event = self.event_queue.get()
            await self.handle_event(event)

    def start_event_loop(self):
        asyncio.run(self.run_event_loop())

    def add_event(self, event):
        self.event_queue.put(event)

    def log_error(self, error_message):
        logging.error(error_message)

    def log_info(self, info_message):
        logging.info(info_message)

# Simulación de la ejecución
notebook = Notebook()

# Añadir celdas
notebook.add_cell("print('Hello, world!')")
notebook.add_cell("print('Another cell')")

# Añadir eventos con diferentes prioridades
notebook.add_event(Event(1, 'execute_cell', 0, time.time()))
notebook.add_event(Event(2, 'execute_cell', 1, time.time()))

# Iniciar el bucle de eventos en un hilo separado
event_loop_thread = threading.Thread(target=notebook.start_event_loop)
event_loop_thread.start()

# Esperar a que las celdas se ejecuten
time.sleep(5)

