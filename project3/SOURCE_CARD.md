# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:** Caroline Brewczak
**Date:** Thu 05 Nov

## 1. The source
- Source and URL: 
    - **Bluesky posts (jetstream)**
    - *wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post*
- Method (WebSocket push, REST polling or REST history pull):
    - **WebSocket push**
- Account, key or User-Agent needed
    - **No Bluesky account or API key needed. It is public**
- Limits I know about (rate limits, paging, reconnects)
    - **No limits from probe run with this rate.**
    -**Some posts have missing fields.**

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
  "did": "did:plc:lw2dxgqsovzs5wevhbz2zj75",
  "time_us": 1794838881953687,
  "cursor": 27246483014,
  "kind": "commit",
  "commit": {
    "rev": "3n2gpyzpf672x",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3n2gpyze3ec2o",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-16T14:21:21.430Z",
      "langs": [
        "en"
      ],
      "reply": {
        "parent": {
          "cid": "bafyre" //shortened long message
          "uri": "at://did:plc:codfx2epdduamfycuyi5fjpb/app.bsky.feed.post/3n2gpluix4k2d"
        },
        "root": {
          "cid": "bafyre" //shortened long message
          "uri": "at://did:plc:codfx2epdduamfycuyi5fjpb/app.bsky.feed.post/3n2gpluix4k2d"
        }
      },
      "text": "“Testerical” is the perfect word👌"
    },
    "cid": "bafyre
  ... (cut)


```

## 3. Volume maths
- Measured rate (from the probe): 
    - **51.18** per **second**
- Live capture needed for 50,000: 
    - **977 seconds (16min)**
- History / backfill available? How many days? 
    - **Yes. Jetstream allows you to lookback/backfill a limited amount. This lookback window is limited based on day but 36 hours (3 days) is the upper bound.**
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov)
    - **Run the producer for about thirty minutes since it takes about 16 minutes to get 50,000 raw records. This extra time is needed because the rate 51.18 is not gauranteed, it is the rate from the short probe. Rate depends on things like time of day and how many users are active because its based on posting activity. Additionally, the probe could crash or disconnect, but Jetstream’s cursor can replay some window of events after reconnecting, so they are not completely lost.**
    


## 4. Topic design
- Topicname
    - "bluesky_posts" (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key
    - **did** will be the key to decide which partition the message goes into. 
- Why this key (ordering, spread across partitions)
    - **Because it identifies the account that generated the event. Using it as the Kafka key keeps all events from the same account in the same partition and preserves their order.**
    - **It also spreads them. There are many different Bluesky accounts, meaning there are many different *did* values. Kafka can hash those different values across the three partitions. So it's not forcing the entire Bluesky stream into one partition.**

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander)
    - **Kafka stores a message with a topic (bluesky_posts), partition (partition #), and offset (position in partition)**
    - **If that same combination of three values is seen twice in the data, the lander duplicated the Kafka record.**
- What makes two records **the same thing** (the same trade, post, aircraft position)?
    - **If the identitiy is the same. In other words, if the key did (the Bluesky account) and commit.rkey (identifies that particular record/post within the account) are the same**
- If the same thing can arrive in several versions, which one do I keep?
    - **Keep the latest event for each post based on did + commit.rkey. If the latest operation is a delete, exclude that post from the final dataset. Otherwise, keep the most recent version containing the post’s language and creation time.**

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join:

**How does Bluesky posting activity vary by language and hour of day?** (need time_us and langs)

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
