import os
import time
import json
import pandas as pd
import openai

# pip install openai
# 1) Set your API key
openai.api_key = os.getenv("OPENAI_API_KEY")

# Common retry/backoff wrapper
from openai import error as OpenAIError

def call_openai(messages, model, max_tokens, temperature=0.0):
    backoff = [1, 2, 4]
    for delay in backoff:
        try:
            return openai.ChatCompletion.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
        except OpenAIError.RateLimitError:
            time.sleep(delay)
        except OpenAIError.InvalidRequestError:
            # Propagate context-lengths to caller
            raise
    raise RuntimeError("OpenAI API call failed after retries")

# 2) Load data
INPUT_PATH = r'C:\Data.xlsx'
df = pd.read_excel(INPUT_PATH)

# 3) Prepare output columns
for col in ["key_themes", "common_questions", "item_ranking", "max_supplements", "lifestyle_changes", "emotional_states", "misconceptions"]:
    df[col] = None

# 4) Define columns
original_col = "Original_post"
comment_cols = [c for c in df.columns if c.startswith("Comment_")]

# 5) Summarization parameters & helpers
MAX_WORDS = 2000
CHUNK_SIZE = 1000
SUMMARY_TOKENS = 500

def count_words(text: str) -> int:
    return len(text.split())

def chunk_text(text: str, size: int) -> list:
    words = text.split()
    return [" ".join(words[i:i+size]) for i in range(0, len(words), size)]

def summarize_text(text: str) -> str:
    prompt = (
        "Summarize this text into up to 3 concise paragraphs, preserving key points and "
        "including dietary items, emotional states, concerns, supplements, and lifestyle changes:\n\n" + text
    )
    messages = [
        {"role":"system","content":"You are a concise summarizer, with cervical cancer HPV expertise."},
        {"role":"user","content":prompt}
    ]
    resp = call_openai(messages, model="gpt-3.5-turbo", max_tokens=SUMMARY_TOKENS, temperature=0.5)
    return resp.choices[0].message.content.strip()

def get_condensed(comments: str) -> str:
    if count_words(comments) <= MAX_WORDS:
        return comments
    parts = chunk_text(comments, CHUNK_SIZE)
    sums = []
    for part in parts:
        sums.append(summarize_text(part))
        time.sleep(1)
    merged = "\n\n".join(sums)
    if count_words(merged) > MAX_WORDS:
        return summarize_text(merged)
    return merged

# 6) Analysis with retry on context length
analysis_template = (
    "You are a content analysis and NLP assistant. Extract:\n"
    "1. Key themes (list with emergence probability).\n"
    "2. Common questions (list with emergence probability).\n"
    "3. Dietary/nutritional items ranked by frequency.\n"
    "4. Highest number of different supplements taken by a single individual, along with items.\n"
    "5. Common lifestyle changes (non-dietary) with emergence probability.\n"
    "6. Most frequently expressed emotional states with emergence probability.\n"
    "7. Most commonly emerging misconceptions related to HPV and abnormal screening.\n\n"
)

def analyze_thread(orig: str, comm: str) -> dict:
    def build_msgs(o, c):
        return [
            {"role":"system","content":"You are an expert in thematic analysis and NLP, with cervical cancer HPV expertise."},
            {"role":"user","content":(
                analysis_template +
                f"Original post:\n\"\"\"{o}\"\"\"\n\n" +
                f"Comments summary:\n\"\"\"{c}\"\"\"\n\n" +
                "Respond ONLY in VALID JSON keys: themes, questions, item_ranking, max_supplements, lifestyle_changes, emotional_states, misconceptions."
            )}
        ]
    # try normal
    try:
        resp = call_openai(build_msgs(orig, comm), model="gpt-4", max_tokens=2000)
    except OpenAIError.InvalidRequestError:
        # fallback: summarize both orig+comm heavily
        fallback = summarize_text(orig + "\n\n" + comm)
        resp = call_openai(build_msgs("", fallback), model="gpt-4", max_tokens=2000)
    raw = resp.choices[0].message.content
    j = raw.find("{"); k = raw.rfind("}")+1
    return json.loads(raw[j:k])

# 7) Main loop
for i, row in df.iterrows():
    orig = str(row[original_col])
    comm = "\n".join(str(row[c]) for c in comment_cols if pd.notna(row[c]))
    short = get_condensed(comm)
    try:
        result = analyze_thread(orig, short)
        df.at[i, "key_themes"]      = result.get("themes", [])
        df.at[i, "common_questions"] = result.get("questions", [])
        df.at[i, "item_ranking"]    = result.get("item_ranking", [])
        df.at[i, "max_supplements"] = result.get("max_supplements")
        df.at[i, "lifestyle_changes"]= result.get("lifestyle_changes", [])
        df.at[i, "emotional_states"] = result.get("emotional_states", [])
        df.at[i, "misconceptions"]  = result.get("misconceptions", [])
    except Exception as e:
        print(f"Row {i}: error: {e}")
    time.sleep(1)

# 8) Save Excel
df.to_excel("Data_with_analysis_row.xlsx", index=False)
print("Done: Data_with_analysis_row.xlsx saved.")
