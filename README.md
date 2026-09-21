# de-cdc-streaming-data-pipeline
Real-Time CDC Event Streaming Pipeline | PostgreSQL → Debezium → Kafka → Spark Structured Streaming → Snowflake


Project 1 taught us **timestamp-based incremental loading**.
Project 2 taught us **moving that incremental pattern into Snowflake**.

Project 3 should answer the question:
**How does a real production system capture INSERT, UPDATE, and DELETE events from PostgreSQL and continuously move them through Kafka into a streaming processing layer?**

### Architecture

```
PostgreSQL
    │
    │ database changes
    ▼
Debezium
    │
    │ CDC events
    ▼
Kafka
    │
    │ continuous stream
    ▼
Spark Structured Streaming
    │
    │ transform / process
    ▼
Snowflake
```

### Project 3 — Real-Time CDC Pipeline

#### Objective

Build an end-to-end **Change Data Capture (CDC)** pipeline that:
- Captures PostgreSQL INSERT, UPDATE and DELETE operations
- Uses Debezium to read PostgreSQL's transaction log
- Publishes CDC events to Kafka
- Uses Spark Structured Streaming to continuously consume Kafka
- Understands Kafka topics, partitions, offsets and consumer groups
- Parses Debezium CDC events in Spark
- Handles INSERT / UPDATE / DELETE correctly
- Uses Spark checkpoints for recovery
- Loads the processed changes into Snowflake

The most important learning here will be:
**Debezium captures the change. 
Kafka transports the change. 
Spark processes the change. 
Snowflake stores the result.**

# Architecture we'll build

```
                         PostgreSQL
                         customers
                             │
                             │
                    INSERT / UPDATE / DELETE
                             │
                             ▼
                       PostgreSQL WAL
                             │
                             ▼
                         Debezium
                       CDC Connector
                             │
                             ▼
                           Kafka
                             │
                  ┌──────────┴──────────┐
                  │                     │
             topic/partition       offsets
                  │
                  ▼
          Spark Structured
             Streaming
                  │
          ┌───────┴────────┐
          │                │
       INSERT/UPDATE     DELETE
          │                │
          └───────┬────────┘
                  ▼
             Snowflake
                  │
                  ▼
             Target Table
```

---

# What makes Project 3 different?

**Project 1 & 2**
We asked PostgreSQL:
`WHERE updated_at > watermark`

So PostgreSQL had to tell us what changed.
The pipeline couldn't see deletes.

---
**Project 3**

We won't ask:
`WHERE update_at > ...`

Instead:
```
PostgreSQL transaction
        ↓
PostgreSQL WAL
        ↓
Debezium
```

Debezium observes database changes from the transaction log.
So:
```
INSERT → captured
UPDATE → captured
DELETE → captured
```

That's the fundamental CDC difference we want you to experience hands-on.

---
### The project phases

I suggest we do this in **8 phases**.

| Phase   | Goal                                     |
| ------- | ---------------------------------------- |
| Phase 1 | Understand CDC + PostgreSQL WAL          |
| Phase 2 | Set up Kafka                             |
| Phase 3 | Set up Debezium                          |
| Phase 4 | Capture PostgreSQL changes               |
| Phase 5 | Understand the Debezium Kafka event      |
| Phase 6 | Build Spark Structured Streaming         |
| Phase 7 | Apply CDC changes to Snowflake           |
| Phase 8 | Failure recovery + end-to-end validation |

And we'll actually **stop after each major phase and verify it**.

---
### Phase 1 — Understand CDC

Before installing anything, we'll establish the mental model.

We'll create: `customers`
in PostgreSQL.

For example:
```
101 | Nishant | Bangalore
102 | Rahul   | Chennai
103 | Amit    | Delhi
```

Then we'll perform:
```
INSERT
UPDATE
DELETE
```

and observe what happens. The important question will be:

> **Where does PostgreSQL record these changes?**

That leads us to the **[[WAL]] — Write-Ahead Log**.
Then **Debezium** comes into the picture.

---

### Phase 2 — Kafka

You already know the basic:
`Producer → Kafka → Consumer`

Project 3 will make that much more concrete.

We'll understand:
```
Kafka
 └── topic
      ├── partition 0
      ├── partition 1
      └── ...
```

And:
```
event
 ↓
partition
 ↓
offset
```

But importantly, **we won't manually type events into a Kafka producer**.

Debezium will become our producer.
```
PostgreSQL
    ↓
Debezium
    ↓
Kafka
```

So when you execute:
```
UPDATE customers
SET city = 'Hyderabad'
WHERE customer_id = 101;
```

you should eventually be able to see a corresponding Kafka event.
That's where Kafka will become meaningful.

---

### Phase 3 — Debezium

We'll run Debezium locally and configure a PostgreSQL connector.

Conceptually:
```
Debezium PostgreSQL Connector
             │
             ▼
       PostgreSQL WAL
             │
             ▼
       CDC Event
             │
             ▼
           Kafka
```

We'll learn the important Debezium concepts:
- Connector
- Source connector
- Database server name
- Connector configuration
- Snapshot
- WAL/LSN
- CDC event
- Before state
- After state
- Operation type

---
### Phase 4 — Actually Generate CDC

This is where we'll spend significant time.

We'll perform:

**INSERT** 
`INSERT INTO customers...`

and inspect the event.

**UPDATE**
```
UPDATE customers
SET city = 'Hyderabad'
WHERE customer_id = 101;
```

and inspect:
`before`
`after`
 
 **DELETE**
```
DELETE FROM customers
WHERE customer_id = 101;
```
and inspect what Debezium sends.

You'll see why CDC is different from timestamp incremental loading.

---
#### Phase 5 — Understand the Debezium Event

This is extremely important before touching Spark.
A Debezium event will contain metadata plus the actual change.

Conceptually:

```
{
  "before": {...},
  "after": {...},
  "source": {...},
  "op": "u"
}
```
The operation code tells us what happened.

Conceptually:
```
c → create
u → update
d → delete
r → read/snapshot
```

We'll inspect the **actual event generated by our environment** rather than memorizing JSON from a tutorial.

---

### Phase 6 — Spark Structured Streaming

This is the **core learning objective** of Project 3.

We'll build:
```
Kafka
   │
   ▼
Spark Structured Streaming
   │
   ▼
parse Debezium event
   │
   ▼
determine operation
   │
   ├── INSERT
   ├── UPDATE
   └── DELETE
```
We'll learn hands-on:

**Streaming DataFrame**
`spark.readStream`

versus our previous batch:
`spark.read`

**Kafka** **source**
Spark will continuously consume Kafka.

**Trigger**
We'll understand when Spark processes available records.

**Checkpoint**
This is critical.
```
Kafka offset
      +
Spark checkpoint
```

We'll stop Spark.
Then restart it.
And see how it resumes instead of blindly starting from the beginning.

---

### Phase 7 — Snowflake

Finally:
```
PostgreSQL
    ↓
Debezium
    ↓
Kafka
    ↓
Spark Streaming
    ↓
Snowflake
```

We'll apply:
```
INSERT → INSERT
UPDATE → UPDATE
DELETE → DELETE
```
to the Snowflake target.

So if PostgreSQL starts as:
```
101 | Nishant | Bangalore
102 | Rahul   | Chennai
```

Then:
```
UPDATE customers
SET city = 'Hyderabad'
WHERE customer_id = 101;
```

will eventually result in:
```
101 | Nishant | Hyderabad
```
in Snowflake.

And if:
```
DELETE FROM customers
WHERE customer_id = 101;
```
then the Snowflake record should disappear as well.

**That will be our real CDC demonstration.**

---
### Phase 8 — Break the system

This is where we'll make sure we actually understand streaming.

We'll test:
```
1. New INSERT
2. UPDATE
3. DELETE
4. Multiple events
5. Stop Spark
6. Generate events while Spark is down
7. Restart Spark
8. Checkpoint recovery
9. Duplicate/replayed events
10. Initial Debezium snapshot
```

We don't need to do all of these immediately.
We'll encounter them naturally as we build.

---
### One important architecture decision

For this project, I recommend we run the infrastructure **locally**:
```
PostgreSQL
Kafka
Debezium
Spark
```
and use your existing Snowflake account as the cloud target.

That gives us:
```
Local source + local streaming infrastructure
                    ↓
                 Snowflake
```

This is excellent for hands-on learning because you won't be paying for Kafka infrastructure just to experiment.

---

### Final project structure

Something along these lines:
```
project_03_cdc_pipeline/
│
├── kafka/
│   └── ...
│
├── debezium/
│   └── connector-config.json
│
├── spark/
│   ├── cdc_stream.py
│   └── schemas.py
│
├── sql/
│   ├── source_setup.sql
│   └── snowflake_setup.sql
│
├── data/
│   └── checkpoints/
│
├── jars/
│   └── ...
│
├── config.py
├── requirements.txt
└── README.md
```
We can adjust the structure once we start implementing rather than prematurely creating files.

---
### The learning progression

The most important thing is that we **don't jump directly to Spark**.

We'll follow the actual data flow:
```
                    PROJECT 3

       "Something changed in PostgreSQL"
                       │
                       ▼
                  PostgreSQL
                       │
                       ▼
                      WAL
                       │
                       ▼
                   Debezium
                       │
              "I captured the change"
                       │
                       ▼
                     Kafka
                       │
              "I transport the event"
                       │
                       ▼
             Spark Streaming
                       │
              "I process the event"
                       │
                       ▼
                  Snowflake
                       │
              "I store the result"
```

By the end, you should be able to explain **why each component exists**, not just how to configure it.