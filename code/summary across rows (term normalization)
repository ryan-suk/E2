import os
import sys
import time
import json
import ast
import re
import pandas as pd
import numpy as np
import openai
from difflib import SequenceMatcher

# -------------------------------
# Configuration
# -------------------------------
INPUT_XLSX    = r'C:\Data2.xlsx'                  # adjust path as needed
OUTPUT_XLSX   = 'aggregated_analysis_normalized.xlsx'

# OpenAI settings
SUMMARY_MODEL  = 'gpt-3.5-turbo'
SUMMARY_TOKENS = 200

# -------------------------------
# Setup OpenAI
# -------------------------------
if not os.getenv('OPENAI_API_KEY'):
    sys.exit('Error: OPENAI_API_KEY environment variable not set.')
openai.api_key = os.getenv('OPENAI_API_KEY')

# -------------------------------
# Helpers
# -------------------------------

def parse_cell(cell):
    """Parse a cell into list of primitives or dicts"""
    if pd.isna(cell): return []
    if isinstance(cell, (list, tuple)): return list(cell)
    txt = str(cell).strip()
    if txt.startswith('[') and txt.endswith(']'):
        try: return json.loads(txt)
        except: pass
        try:
            val = ast.literal_eval(txt)
            if isinstance(val, (list, tuple)): return list(val)
        except: pass
    if txt.startswith('{') and txt.endswith('}'):
        try: return [json.loads(txt.replace("'", '"'))]
        except: pass
        try:
            val = ast.literal_eval(txt)
            if isinstance(val, dict): return [val]
        except: pass
    return [s.strip() for s in txt.split(',') if s.strip()]


def normalize_term(term):
    """Lowercase, remove punctuation, extra spaces."""
    t = term.lower()
    t = re.sub(r"[^a-z0-9 ]+", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def cluster_normalized(keys, threshold=0.6):
    """Cluster normalized keys by similarity, return mapping to representative."""
    reps = []
    mapping = {}
    for k in keys:
        found = False
        for r in reps:
            if SequenceMatcher(None, k, r).ratio() >= threshold:
                mapping[k] = r
                found = True
                break
        if not found:
            reps.append(k)
            mapping[k] = k
    return mapping

# -------------------------------
# Load Data
# -------------------------------
try:
    df = pd.read_excel(INPUT_XLSX)
except Exception as e:
    sys.exit(f"Error loading '{INPUT_XLSX}': {e}")

# -------------------------------
# Aggregate with normalization
# -------------------------------
results = []
for col in df.columns:
    freq = {}
    # collect raw freq
    for cell in df[col]:
        for it in parse_cell(cell):
            if isinstance(it, dict):
                # pick first key, value
                if 'item' in it:
                    key = it['item']; count = it.get('count', it.get('frequency',1))
                elif 'state' in it:
                    key = it['state']; count = it.get('probability',1)
                elif 'misconception' in it:
                    key = it['misconception']; count = it.get('frequency', it.get('probability',1))
                elif 'number' in it and 'items' in it:
                    key = 'max_supplements'; count = it.get('number',1)
                else:
                    key = json.dumps(it); count = 1
            else:
                key = str(it); count = 1
            freq[key] = freq.get(key,0) + count
    # normalize keys
    norm_freq = {}
    for key, cnt in freq.items():
        nk = normalize_term(key)
        norm_freq[nk] = norm_freq.get(nk,0) + cnt
    # cluster normalized keys
    mapping = cluster_normalized(list(norm_freq.keys()), threshold=0.6)
    # aggregate clusters
    cluster_counts = {}
    for nk, cnt in norm_freq.items():
        rep = mapping[nk]
        cluster_counts[rep] = cluster_counts.get(rep,0) + cnt
    # build aggregated_items sorted
    agg_items = [{'item': rep, 'count': cluster_counts[rep]}
                 for rep in sorted(cluster_counts, key=lambda r: cluster_counts[r], reverse=True)]
    # summary via GPT
    top_lines = '\n'.join(f"- {i['item']}: {i['count']}" for i in agg_items[:10])
    prompt = (
        f"Column '{col}' aggregated (normalized & merged) items and counts:\n{top_lines}\n\n"
        "Write a concise paragraph summarizing these patterns."
    )
    try:
        resp = openai.ChatCompletion.create(
            model=SUMMARY_MODEL,
            messages=[
                {'role':'system','content':'You are a concise summarizer.'},
                {'role':'user','content':prompt}
            ],
            temperature=0.0,
            max_tokens=SUMMARY_TOKENS
        )
        summary = resp.choices[0].message.content.strip()
    except:
        summary = ''
    results.append({'column':col, 'aggregated_items':agg_items, 'summary':summary})

# -------------------------------
# Save Results
# -------------------------------
out_df = pd.DataFrame(results)
with pd.ExcelWriter(OUTPUT_XLSX, engine='openpyxl') as w:
    df.to_excel(w, sheet_name='Raw_Data', index=False)
    out_df.to_excel(w, sheet_name='Aggregated_Results', index=False)

print(f"Aggregation complete. Results saved to '{OUTPUT_XLSX}'")
