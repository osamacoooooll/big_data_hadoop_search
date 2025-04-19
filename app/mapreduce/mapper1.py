#!/usr/bin/env python3
import sys
import re

WORD_RE = re.compile(r"\w+")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    # Expect: <doc_id>\t<doc_title>\t<doc_text>
    parts = line.split('\t', 2)
    if len(parts) != 3:
        continue
    doc_id, _, text = parts

    # tokenize & count
    terms = WORD_RE.findall(text.lower())
    dl = len(terms)
    freq = {}
    for t in terms:
        freq[t] = freq.get(t, 0) + 1

    # emit term → doc_id:tf:dl
    for term, tf in freq.items():
        print(f"{term}\t{doc_id}:{tf}:{dl}")
