#!/usr/bin/env python

import smallhands.main
import sys


if __name__ == "__main__":
    sh = None
    try:
        sh = smallhands.main.Smallhands()
        sh.start()
    except Exception, e:
        sys.exit(1)
    finally:
        if sh:
            sh.stop()
