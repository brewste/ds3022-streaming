"""Earthquakes -> Kafka topic "quakes".

  python quakes_producer.py history 7   # PULL: the last 7 days from the EMSC catalogue
  python quakes_producer.py live        # PUSH: new and updated events from the EMSC WebSocket

Adapted from Professor Magee's stream-seismic.py (learn-kafka).
"""
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky_posts"
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


def send(producer, message):
    """One Kafka record per message. Key = did, so events from the
    same Bluesky account go to the same partition, in order."""
    key = message["data"].get("did")   # .get(): no crash if missing
    if key is None:
        logging.warning("No did: sent without a key")
    producer.produce(topic=TOPIC, key=key, value=json.dumps(message))


#deleted history function
#I can run live with enough buffer time to get 50,000 raw records, so I will not use a history/backfill function.


async def live(producer):
    """PUSH over a WebSocket, with a reconnect loop."""
    count = 0 #Logging counter for messages sent

    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
            while True:
                text = await ws.read_message()
                if text is None:
                    break
                try:
                    send(
                        producer,
                        {"action": "live", "data": json.loads(text)}
                    )
                    count += 1
                    if count % 1000 == 0:
                        logging.info("%d messages sent to %s", count, TOPIC) #logging every 1000 messages sent for fast sources
                except (ValueError, KeyError, TypeError) as e:
                    logging.warning("Skipped a message: %r", e)

        except Exception as e:
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "live"
    app = Application(broker_address=BROKER, loglevel="WARNING")
   
    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
