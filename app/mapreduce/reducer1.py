#!/usr/bin/env python3
import sys

current_term = None
postings = []

def flush(term, postings):
    df = len(postings)
    post_str = ",".join(postings)
    print(f"{term}\t{df}\t{post_str}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    term, rest = line.split('\t', 1)
    if current_term and term != current_term:
        flush(current_term, postings)
        postings = []
    current_term = term
    postings.append(rest)

# last term
if current_term:
    flush(current_term, postings)
