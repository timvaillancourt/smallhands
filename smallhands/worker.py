import logging

from multiprocessing import Process


class Find(Process):
    def __init__(self, config, queue, do_stop, sleep_secs=0.50):
        Process.__init__(self)
        self.config     = config
        self.queue      = queue
        self.do_stop    = do_stop
        self.sleep_secs = sleep_secs

    def run(self):
        logging.info("Starting .find() worker")
        while not self.do_stop.is_set():
            if not self.queue.empty() and self.queue.qsize() > 10:
                data = self.queue.get(False, 1)
                if data:
                    print("find", data)
