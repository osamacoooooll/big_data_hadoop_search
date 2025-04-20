#!/usr/bin/env python3
import sys, os, re
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

if len(sys.argv) != 2:
    print("Usage: python3 app.py <index_output.txt>")
    sys.exit(1)

index_file = sys.argv[1]
DOCS_DIR = "data"
KEYSPACE = "search_engine"

# Connect to Cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect()

# 1) Create keyspace
session.execute(f"""
  CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
  WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
""")
session.set_keyspace(KEYSPACE)

# 2) Create tables
session.execute("""
  CREATE TABLE IF NOT EXISTS documents (
    doc_id text PRIMARY KEY,
    title text,
    dl int
  )
""")
session.execute("""
  CREATE TABLE IF NOT EXISTS postings (
    term text,
    doc_id text,
    tf int,
    dl int,
    PRIMARY KEY (term, doc_id)
  )
""")
session.execute("""
  CREATE TABLE IF NOT EXISTS term_stats (
    term text PRIMARY KEY,
    df int
  )
""")

# Prepare inserts
ins_doc     = session.prepare("INSERT INTO documents (doc_id, title, dl) VALUES (?, ?, ?)")
ins_posting = session.prepare("INSERT INTO postings (term, doc_id, tf, dl) VALUES (?, ?, ?, ?)")
ins_term    = session.prepare("INSERT INTO term_stats (term, df) VALUES (?, ?)")

WORD_RE = re.compile(r"\w+")

# 3) Load document metadata
print(">> Loading documents metadata...")
for fname in os.listdir(DOCS_DIR):
    if not fname.endswith(".txt"):
        continue
    doc_id, title = fname[:-4].split("_", 1)
    with open(os.path.join(DOCS_DIR, fname), 'r', encoding='utf-8') as f:
        text = f.read()
    dl = len(WORD_RE.findall(text))
    # title stored with spaces
    session.execute(ins_doc, (doc_id, title.replace("_", " "), dl))

# 4) Load postings & term stats from MapReduce output
print(">> Loading index into Cassandra...")
with open(index_file, 'r', encoding='utf-8') as f:
    for line in f:
        term, df_str, postings_str = line.strip().split('\t')
        df = int(df_str)
        session.execute(ins_term, (term, df))
        for p in postings_str.split(','):
            doc_id, tf_str, dl_str = p.split(':')
            session.execute(ins_posting, (term, doc_id, int(tf_str), int(dl_str)))

print(">> Done.")