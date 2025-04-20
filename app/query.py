#!/usr/bin/env python3
import sys, re, math
from cassandra.cluster import Cluster
from pyspark import SparkContext

if len(sys.argv) != 2:
    print("Usage: spark-submit query.py \"your query here\"")
    sys.exit(1)

query = sys.argv[1]
print(query)
TERMS = re.findall(r"\w+", query.lower())

# 1) Connect to Cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

# 2) Compute total docs N and avgdl
#   Note: Cassandra doesn't support avg(), so we pull all dl's
rows = session.execute("SELECT dl FROM documents")
dls = [r.dl for r in rows]
N = len(dls)
avgdl = sum(dls) / float(N)

# 3) Fetch df for each term
df_map = {}
for t in TERMS:
    row = session.execute("SELECT df FROM term_stats WHERE term=%s", (t,)).one()
    df_map[t] = row.df if row else 0

# 4) Fetch all postings for query terms
#    build a list of (term, doc_id, tf, dl, df)
postings = []
for t in TERMS:
    rows = session.execute("SELECT doc_id, tf, dl FROM postings WHERE term=%s", (t,))
    for r in rows:
        postings.append((t, r.doc_id, r.tf, r.dl, df_map[t]))

# 5) Spark RDD BM25 scoring
def bm25(tf, df, dl, avgdl, N, k1=1.0, b=0.75):
    idf = math.log((N + 1) / (df + 1))
    numer = tf * (k1 + 1)
    denom = tf + k1 * (1 - b + b * dl / avgdl)
    return idf * numer / denom

sc = SparkContext(appName="BM25Query")
rdd = sc.parallelize(postings)  # (term, doc_id, tf, dl, df)

# map to (doc_id, score) then sum across terms
scores = (rdd
    .map(lambda rec: (rec[1], bm25(rec[2], rec[4], rec[3], avgdl, N)))
    .reduceByKey(lambda a, b: a + b)
)

top10 = scores.takeOrdered(10, key=lambda x: -x[1])

# 6) Lookup titles & print
print(f"number of docs retrieved:{len(top10)}")
for doc_id, score in top10:
    row = session.execute("SELECT title FROM documents WHERE doc_id=%s", (doc_id,)).one()
    title = row.title if row else "(unknown)"
    print(f"{doc_id}\t{title}\t{score:.4f}")

sc.stop()
