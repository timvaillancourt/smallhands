import logging

from multiprocessing import Process
from Queue import Empty
from time import sleep


class Remove(Process):
    def __init__(self, db, queue, do_stop, min_queued=1):
        Process.__init__(self)
        self.db         = db
        self.queue      = queue
        self.do_stop    = do_stop
        self.min_queued = min_queued

    def run(self):
        logging.info("Starting remove() worker")
        while not self.do_stop.is_set():
            queued = self.queue.qsize()
            if queued >= self.min_queued:
                try:
                    data = self.queue.get_nowait()
                    if data:
                        logging.info("remove: %s (queued=%i)" % (data, queued - self.min_queued))
                        self.db['smallhands'].tweets.remove(data)
                except Empty:
                    pass
            sleep(0.1)
        logging.info("Stopped remove() worker")


class Find(Process):
    def __init__(self, db, queue, do_stop, min_queued=1):
        Process.__init__(self)
        self.db         = db
        self.queue      = queue
        self.do_stop    = do_stop
        self.min_queued = min_queued

    def run(self):
        logging.info("Starting point .find() worker")
        while not self.do_stop.is_set():
            queued = self.queue.qsize()
            if queued >= self.min_queued:
                try:
                    data = self.queue.get_nowait()
                    if data:
                        logging.info("find: %s (queued=%i)" % (data, queued - self.min_queued))
                        self.db['smallhands'].tweets.find_one(data)
                except Empty:
                    pass
            sleep(0.1)
        logging.info("Stopped point .find() worker")
