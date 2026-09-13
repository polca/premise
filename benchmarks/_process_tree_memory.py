"""Optional aggregate RSS sampling for benchmarks with child processes."""

import threading


class ProcessTreeMemory:
    def __init__(self):
        self.peak = None
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        try:
            import psutil
        except ImportError:
            return self
        parent = psutil.Process()
        try:
            parent.children(recursive=True)
        except (psutil.Error, OSError):
            return self

        def sample():
            while not self._stop.is_set():
                total = 0
                try:
                    processes = [parent, *parent.children(recursive=True)]
                except (psutil.Error, OSError):
                    self.peak = None
                    return
                for process in processes:
                    try:
                        total += process.memory_info().rss
                    except (psutil.Error, OSError):
                        pass
                self.peak = max(self.peak or 0, total)
                self._stop.wait(0.1)

        self._thread = threading.Thread(target=sample, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        return self.peak
