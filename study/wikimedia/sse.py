"""Server-Sent Events client for Wikimedia EventStreams, standard library only.

Wikimedia's API etiquette asks for one connection at a time and a User-Agent that
names the client and gives a contact address. EventStreams puts the Kafka position of
every event in its SSE id; reconnecting with that id as Last-Event-ID resumes after the
last event received, so a reconnect does not by itself drop events.
"""
import http.client
import time
import urllib.error
import urllib.request

STREAM_URL = "https://stream.wikimedia.org/v2/stream/mediawiki.page_change.v1"
CONTACT = "https://github.com/spoorthibasu/kafka-flink-audit-trail"
USER_AGENT = "audit-trail-bench-wikimedia-study/1.0 ({}) Python-urllib".format(CONTACT)


def _parse(resp):
    """Yield (type, id, data) for each event dispatched on one open response."""
    ev_type, ev_id, data = None, None, []
    for raw in resp:
        line = raw.decode("utf-8").rstrip("\r\n")
        if not line:
            if data:
                yield ev_type or "message", ev_id, "\n".join(data)
            ev_type, data = None, []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            data.append(value)
        elif field == "event":
            ev_type = value
        elif field == "id":
            # Per the SSE spec the last id persists until the server sends another.
            ev_id = value


def stream(url=STREAM_URL, user_agent=USER_AGENT, last_event_id=None, log=None,
           read_timeout=60.0, max_backoff=60.0, resume_id=None):
    """Yield (type, id, data, recv_ms) forever, reconnecting with Last-Event-ID.

    log(record) receives a dict for every connect and disconnect. Without resume_id,
    each reconnect sends the last id the server sent. With resume_id, each connect
    sends resume_id() instead, so the consumer decides where to resume. The generator
    only reconnects after the consumer has taken every event it yielded.
    """
    log = log or (lambda record: None)
    backoff = 1.0
    while True:
        if resume_id is not None:
            last_event_id = resume_id()
        headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        connected_ms = int(time.time() * 1000)
        n = 0
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=read_timeout) as resp:
                log({"kind": "connected", "ts_ms": connected_ms, "status": resp.status,
                     "resumed": last_event_id is not None, "last_event_id": last_event_id})
                for ev_type, ev_id, data in _parse(resp):
                    recv_ms = int(time.time() * 1000)
                    if ev_type == "message":
                        n += 1
                        backoff = 1.0
                        if ev_id:
                            last_event_id = ev_id
                    yield ev_type, ev_id, data, recv_ms
            reason = "server closed the stream"
        except urllib.error.HTTPError as e:
            reason = "HTTP {}".format(e.code)
            if e.code == 429:
                backoff = max(backoff, 30.0)
        except (OSError, http.client.HTTPException) as e:
            reason = "{}: {}".format(type(e).__name__, e)
        log({"kind": "disconnected", "ts_ms": int(time.time() * 1000), "reason": reason,
             "events_on_connection": n, "connected_ms": connected_ms,
             "last_event_id": last_event_id, "retry_in_s": backoff})
        time.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)
