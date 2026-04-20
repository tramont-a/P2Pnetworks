# schedulers.py
import random
import threading
import time
from typing import Callable, Iterable, Optional, Sequence, Set, Dict, Any


class ChokeScheduler:
    """
    Periodically selects up to k preferred neighbors among those interested and sends CHOKE/UNCHOKE.
    - If we have the full file: choose k randomly among interested [8].
    - Otherwise: choose k by highest download rate in the previous interval (ties broken randomly) [8].
    Excludes the current optimistic-unchoked neighbor from being newly choked [8].
    Logs preferred-neighbor changes via logger.changePreferredNeigbors([...]) [8].
    Translated from ChokeHandler.java [15].
    """

    def __init__(
        self,
        *,
        k: int,
        interval_sec: int,
        have_complete_file: Callable[[], bool],
        get_interested: Callable[[], Iterable[Any]],
        get_download_rates: Callable[[], Dict[Any, int]],
        get_unchoked_set: Callable[[], Set[Any]],
        set_unchoked_set: Callable[[Set[Any]], None],
        get_optimistic_unchoked: Callable[[], Optional[Any]],
        get_handler: Callable[[Any], Any],  # handler must expose send_unchoke(), send_choke(), reset_download_rate()
        logger: Any,                        # must expose changePreferredNeigbors(list[str])
        on_all_done: Optional[Callable[[], bool]] = None,
        on_cancel: Optional[Callable[[], None]] = None,
    ):
        self.k = k
        self.interval_sec = interval_sec
        self.have_complete_file = have_complete_file
        self.get_interested = get_interested
        self.get_download_rates = get_download_rates
        self.get_unchoked_set = get_unchoked_set
        self.set_unchoked_set = set_unchoked_set
        self.get_optimistic_unchoked = get_optimistic_unchoked
        self.get_handler = get_handler
        self.logger = logger
        self.on_all_done = on_all_done or (lambda: False)
        self.on_cancel = on_cancel or (lambda: None)

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="ChokeScheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)

    # ---- internal ----

    def _choose_preferred(self, interested: Sequence[Any]) -> Set[Any]:
        if not interested:
            return set()
        if self.have_complete_file():
            # Randomly choose up to k among interested [8]
            return set(random.sample(list(interested), k=min(self.k, len(interested))))
        # Choose by highest download rate in previous interval [8]
        rates = self.get_download_rates() or {}
        # Shuffle to break ties randomly before sorting
        shuffled = list(interested)
        random.shuffle(shuffled)
        shuffled.sort(key=lambda pid: rates.get(pid, 0), reverse=True)
        return set(shuffled[: min(self.k, len(shuffled))])

    def _run(self) -> None:
        # Optional small initial delay to allow peers to connect
        time.sleep(0.5)
        while not self._stop.is_set():
            try:
                if self.on_all_done() is True:
                    # Cancel further scheduling if the admin reports all peers are complete [8]
                    self.on_cancel()
                    break

                interested = list(self.get_interested() or [])
                current_unchoked = set(self.get_unchoked_set() or set())
                opt = self.get_optimistic_unchoked()

                if interested:
                    new_preferred = self._choose_preferred(interested)

                    # Send UNCHOKE to new preferred not already unchoked (ignore opt) [8]
                    for pid in new_preferred:
                        if pid != opt and pid not in current_unchoked:
                            try:
                                self.get_handler(pid).send_unchoke()
                            except Exception:
                                pass

                    # Send CHOKE to previously unchoked peers not in new preferred and not opt [8]
                    for pid in current_unchoked:
                        if pid not in new_preferred and pid != opt:
                            try:
                                self.get_handler(pid).send_choke()
                            except Exception:
                                pass

                    # Update set and log
                    self.set_unchoked_set(set(new_preferred))
                    if new_preferred:
                        # Logger expects strings
                        self.logger.changePreferredNeigbors([str(p) for p in new_preferred])

                    # Reset download-rate counters for the newly preferred set (next interval accounting) [15][8]
                    for pid in new_preferred:
                        try:
                            self.get_handler(pid).reset_download_rate()
                        except Exception:
                            pass
                else:
                    # No interested peers: choke all except possibly the optimistic one, and clear preferred set [15]
                    for pid in list(current_unchoked):
                        if pid != opt:
                            try:
                                self.get_handler(pid).send_choke()
                            except Exception:
                                pass
                    self.set_unchoked_set(set())

            except Exception:
                # Keep scheduler alive on unexpected errors
                pass

            # Sleep until next interval
            self._stop.wait(self.interval_sec)


class OptimisticUnchokeScheduler:
    """
    Periodically selects one interested-but-currently-choked neighbor to optimistically unchoke [8].
    Ensures the previous optimistic neighbor is choked if it is not in the preferred set anymore [18][8].
    Logs via logger.changeOptimisticallyUnchokedNeighbor(peerId) [8].
    Translated from OptimisticUnchokeHandler.java [18].
    """

    def __init__(
        self,
        *,
        interval_sec: int,
        get_interested: Callable[[], Iterable[Any]],
        get_unchoked_set: Callable[[], Set[Any]],
        get_optimistic_unchoked: Callable[[], Optional[Any]],
        set_optimistic_unchoked: Callable[[Optional[Any]], None],
        get_handler: Callable[[Any], Any],   # handler must expose send_unchoke(), send_choke()
        logger: Any,                         # must expose changeOptimisticallyUnchokedNeighbor(str)
        on_all_done: Optional[Callable[[], bool]] = None,
        on_cancel: Optional[Callable[[], None]] = None,
    ):
        self.interval_sec = interval_sec
        self.get_interested = get_interested
        self.get_unchoked_set = get_unchoked_set
        self.get_optimistic_unchoked = get_optimistic_unchoked
        self.set_optimistic_unchoked = set_optimistic_unchoked
        self.get_handler = get_handler
        self.logger = logger
        self.on_all_done = on_all_done or (lambda: False)
        self.on_cancel = on_cancel or (lambda: None)

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="OptimisticUnchokeScheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)

    # ---- internal ----

    def _run(self) -> None:
        time.sleep(0.5)
        while not self._stop.is_set():
            try:
                if self.on_all_done() is True:
                    self.on_cancel()
                    break

                interested = set(self.get_interested() or set())
                preferred = set(self.get_unchoked_set() or set())
                current_opt = self.get_optimistic_unchoked()

                # Candidates are interested peers who are currently choked (not in preferred, not current opt)
                candidates = list(interested - preferred - ({current_opt} if current_opt else set()))

                if candidates:
                    next_opt = random.choice(candidates) if candidates else None
                    # Unchoke the new optimistic neighbor
                    if next_opt is not None:
                        try:
                            self.get_handler(next_opt).send_unchoke()
                        except Exception:
                            pass
                        self.set_optimistic_unchoked(next_opt)
                        self.logger.changeOptimisticallyUnchokedNeighbor(str(next_opt))

                        # If previous opt exists and is not preferred, choke it
                        if current_opt is not None and current_opt not in preferred:
                            try:
                                self.get_handler(current_opt).send_choke()
                            except Exception:
                                pass
                else:
                    # No candidates: clear optimistic neighbor, choke previous if not preferred
                    if current_opt is not None and current_opt not in preferred:
                        try:
                            self.get_handler(current_opt).send_choke()
                        except Exception:
                            pass
                    self.set_optimistic_unchoked(None)

            except Exception:
                pass

            self._stop.wait(self.interval_sec)


class TerminateWatcher:
    """
    Polls a completion predicate and, once complete, invokes a shutdown callback.
    This mirrors the role of TerminateHandler.java at a simpler level [13].
    """

    def __init__(
        self,
        *,
        poll_sec: int,
        is_done: Callable[[], bool],
        on_done: Callable[[], None],
    ):
        self.poll_sec = poll_sec
        self.is_done = is_done
        self.on_done = on_done
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="TerminateWatcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.is_done():
                    try:
                        self.on_done()
                    finally:
                        break
            except Exception:
                pass
            self._stop.wait(self.poll_sec)