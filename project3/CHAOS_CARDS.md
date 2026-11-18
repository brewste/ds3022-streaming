# Chaos cards (checkpoint D, Session 2 · Mon 09 Nov)

Break your own pipeline on purpose and show, with numbers, that it does not lose data. **D1 and D2 are compulsory; do at least one more.** Each card becomes one row of the **Chaos round** table in your README.

## How to run a card

1. **Numbers before.** Run the flow and write down the four numbers:
   ```
   python flow.py
   ```
   raw records landed · malformed · unique Kafka records · unique entities
2. **Predict.** Write in one sentence what you expect to change, *before* you do anything.
3. **Do** the steps on the card.
4. **Numbers after.** Run the flow again and write the four numbers down.
5. **Explain** the difference in one or two sentences, using the words on the card.
6. **Copy the row** into the README table: card · prediction · what I did · before → after · explanation.

Terminals as on Mon 02 Nov: **T1** producer, **T2** lander, **T3** flow, **T4** `rpk`, `docker`, `git`. Run project commands from the `project3` folder.

---

## D1 · Crash between the file and the offsets (compulsory)

*Same idea as card A on Mon 02 Nov, now on your own lander.*

**Numbers before:**  
raw 72,291 | malformed 0 | unique Kafka records 72,291 | unique did+rkey 69,413

**Prediction:**  

- I predict that, after crashing the lander (this happens after the Parquet file was already written but then before storing the Kafka offsets), some records will be landed again after restarting the lander. 
- So therefore raw records will increase more than unique Kafka records. 


**Do steps**

1. T2: stop the lander (Ctrl+C).
2. T1: send some records again: re-run your backfill for a short range, or let the live producer run for a minute.
3. T2: `python lander.py crash` → it writes one file, prints `CRASH (on purpose)` and exits **before** storing offsets.
4. T2: `python lander.py` → it reads the same records again and writes them to a second file.
5. T3: `python flow.py`


**Numbers after:**  
raw 82,772 | malformed 0 | unique Kafka records 80,772 | unique did+rkey 78,675

**Explanation:**  
raw  +10,481 | malformed 0 | unique Kafka records +8,481  | unique did+rkey + 9,262

- The results support my predictino because raw records increase more than unique Kafka records did rather than stayingthe same. 
- Explanation: This pipeline utilizes at-least-once processing. Which means, a message can be processed more than once, but the system prioritizes avoiding data loss. Writing into a parque file comes first, and then storing (marking kafka position), and then committing (saving position in kafka which is where the lander will pick up again last time)
   - The parquet file was saved first, but then the kafka consumer group was not updated for that batch because it crashed. 
   - So Kafka checks the last committed offset, this would be from the previous lander run (not the crashed one). So it then reads and writes them again.
   - That means that there will be more raw records, but they won't all be unique because some were read twice. 


## D2 · A poison message (compulsory)

*Same idea as card B on Mon 02 Nov.*

**Numbers before:**  
raw 82,772 | malformed 0 | unique Kafka records 80,772 | unique did+rkey 78,675


**Prediction:**  
 +1 | +1 | same | Same 

- Even though there is an invalid message type, I think the lander will still run, because it can handle malformed messages. So it will be marked as malformed. 
- Raw records will still technically increase by one because it is all the raw records, but it will not count as the kafka records because its not being stored and commited. 


**Do steps**

1. T4: `docker exec -it redpanda-0 rpk topic produce <your-topic> -k bad`
2. Type `this is not JSON`, press Enter, then Ctrl+C.
3. T2: wait for the next `Landed ... records` line (at most 60 s). The lander must **keep running**.
4. T3: `python flow.py`

Find the row: `python -c "import duckdb; duckdb.sql(\"SELECT kafka_partition, kafka_offset, raw FROM 'data/raw/*.parquet' WHERE action='malformed'\").show()"`

**Numbers after:**  
raw 95,772 | malformed 1 | unique Kafka records 93,771 | unique did+rkey 91,381

**Explanation:**  

- It worked! It handled the invalid JSON message but didn't crash. It was saved as a malformed message; this number went up by 1 
   - The message was saved in a Parquet file and marked as malformed. This demonstrates the principle of “land everything, decide later,” where raw data is preserved even if it cannot be processed normally (doesn't crash)
   - So then the lander is not repeatedly encountering this poison message after restarting, which would just keep blocking the processing. It stores as malformed then proceeds.
- To note: raw records did increase by more than 1 and the other numbers increased as well. However, this is because I ran into a brief error while executing, and therefore executed the lander several times (even without the invalid json message) causing all numbers to increase anyways. But by looking at the proportions of the increasing numbers, we can see that this experiment worked:
   - Raw + 13,000 and unique kafka + 12,999




## D3 · A broker goes down
*Same idea as the broker card on Thu 22 Oct.*

**Numbers before:**  
raw 95,772 | malformed 1 | unique Kafka records 93,771 | unique did+rkey 91,381


**Prediction:**  

- Background info
   - Redpanda is what sits between the producer and lander; its job is to reciever the producer messages, store them in an ordered log, and then make them available.
   - A broker is an individual server that is running this software that stores and serves message. There are multiple (3) so that is one broker crashes, the others will stay operating. 
   - In this pipeline, each parition has three copies distributed across brokers (replication factor of 3), so therefore for each partition has a lead broker (handles main processing) and then replica broker (copies that parition data).

- Prediction
   - Because each broker has a copy of the other paritions, another broker will become the leader. So the data is not lost even though a broker went down because the others kept copies. 
   - This could maybe cause an error or disrupt the process. However data will not be lost, and the producer should be able to continue sending data again after it chooses a new broker candidate. 

**Do steps:**

1. T1: producer running.
2. T4: `docker stop redpanda-1` (never stop `redpanda-0`: it is the address your scripts use).
3. Watch the producer for 60 s, and the Console (`http://127.0.0.1:8080`): partitions show as under-replicated.
4. T4: `docker start redpanda-1`. Wait until the Console is healthy again.
5. T3: `python flow.py`



**Numbers after:**  
*Same because I did not land anything for this chaos card, only did stuff with the producer*

Observations:

- After stopping, runs into an error that says Connection refused. However, it starts back up on its own sending messages.
- After starting redpanda-1 again, it freezes for a moment but then continues sending messages. 


**Explain with:** replication factor 3, leaders move to the brokers that are still up, a majority (2 of 3) is enough to keep writing.

- Replication factor of 3: Each parition has three copies distributed across brokers (replication factor of 3), so therefore for each partition has a lead broker (handles main processing) and then replica broker (copies that parition data).
- Leaders: Because each broker has a copy of the other paritions, another broker will become the leader. So the data is not lost even though a broker went down because the others kept copies. 
- Majority: Majority is enough to keep writing data because there are enough copies of the data remaining. Normally each partition has 3 replicas, but with one down there are only two left. If two went down, one broker would have all copies of the data. 




## Example README row

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D2 Poison message | The lander keeps running; malformed goes from 0 to 1 | `rpk topic produce coinbase_trades -k bad`, typed text, waited for the next file, ran the flow | raw 61,204 → 61,205; malformed 0 → 1; unique records 58,930 → 58,930; unique trades 58,911 → 58,911 | `to_row()` caught the JSON error and landed the text with `action = "malformed"`, so the lander did not crash and the partition was not blocked. The flow keeps it in raw and leaves it out of the clean tables. |

