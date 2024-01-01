from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import subprocess
import config.config as cnf
import os

class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        # run main.py with the new file path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        subprocess.run(['python', os.path.join(script_dir, 'main.py'), event.src_path])

folder_to_monitor = cnf.input_root

event_handler = MyHandler()
observer = Observer()
observer.schedule(event_handler, folder_to_monitor, recursive=True)
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
    observer.join()