#!/usr/bin/env python

import signal
import smallhands
import sys


if __name__ == "__main__":
    sh = None
    try:
        sh = smallhands.Smallhands()
        signal.signal(signal.SIGINT, sh.stop)
        signal.signal(signal.SIGTERM, sh.stop)
        sh.start()
    except Exception, e:
        sys.exit(1)
    finally:
        if sh:
            sh.stop()
