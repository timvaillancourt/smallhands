import logging

from multiprocessing import Process


class Find(Process):
    def __init__(self, config, queue, do_stop, sleep_secs=0.50, min_queued=5):
        Process.__init__(self)
        self.config     = config
        self.queue      = queue
        self.do_stop    = do_stop
        self.sleep_secs = sleep_secs
        self.min_queued = min_queued

    def run(self):
        logging.info("Starting point .find() worker")
        while not self.do_stop.is_set():
            queued = self.queue.qsize()
            if queued > self.min_queued:
                data = self.queue.get_nowait()
                if data:
                    print("find: %s (queued=%i)" % (data, queued - self.min_queued))
        logging.info("Stopped point .find() worker")
