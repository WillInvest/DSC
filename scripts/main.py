#!/usr/bin/env python3
"""DSC pipeline entrypoint; see pipeline/README.md."""
from pipeline.main import main

if __name__=='__main__':
    import signal
    def stop(signum,frame):raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    main()
