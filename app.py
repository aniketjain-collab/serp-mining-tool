"""
SERP Mining & Aggregation Tool
Streamlit app that searches Google via SerpAPI for primary + up to 3 secondary
keywords, aggregates PAA questions and competitor URLs with priority-first
ranking (primary keyword always wins), and provides CSV downloads.
Logic ported from BOMBORA Fan Out n8n workflow.
"""

import streamlit as st
import requests
import pandas as pd
import re

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="SERP Mining Tool",
    page_icon="🔍",
    layout="wide",
)

st.title("SERP Mining & Aggregation Tool")
st.caption(
    "Pulls top People Also Ask questions and competitor URLs from Google "
    "via SerpAPI. Primary keyword always wins (priority-first ranking)."
)

# -----------------------------
# Sidebar — API key
# -----------------------------
with st.sidebar:
    st.header("Configuration")
    serpapi_key = st.text_input(
        "SerpAPI Key",
        type="password",
        help="Your SerpAPI key. Get one at serpapi.com",
    )
    st.markdown("---")
    st.markdown("**How it works**")
    st.markdown(
        "1. Searches Google for your primary keyword + up to 3 secondaries\n"
        "2. Extracts top 5 organic URLs and all PAA questions per search\n"
        "3. Deduplicates and ranks: primary keyword wins, frequency breaks ties\n"
        "4. Returns top 5 PAAs and top 5 URLs with downloadable CSVs"
    )

# -----------------------------
# Input form
# -----------------------------
with st.form("serp_form"):
    report_name = st.text_input(
        "Report Name",
        placeholder="e.g. AI Sales Platform Brief - May 2026",
    )
    primary_keyword = st.text_input(
        "Primary Keyword",
        placeholder="e.g. AI sales platform",
    )
    secondary_keywords = st.text_area(
        "Secondary Keywords (one per line, top 3 used)",
        placeholder="ai sdr platform\nai sales agent\nconversational ai for sales",
        height=120,
    )
    submitted = st.form_submit_button("Run SERP Analysis", type="primary")


# -----------------------------
# Core logic (ported from n8n)
# -----------------------------
def split_keywords(primary: str, secondary_text: str):
    """Split keywords with priority assignment.
    Primary = priority 1. Secondaries = 2, 3, 4 in order. Max 3 secondaries."""
    keywords = []
    if primary and primary.strip():
        keywords.append(
            {"keyword": primary.strip(), "type": "primary", "priority": 1}
        )
    if secondary_text:
        # Split on newlines or commas, trim, drop blanks, cap at 3
        secondary_list = [
            k.strip() for k in re.split(r"[,\n]", secondary_text) if k.strip()
        ][:3]
        for i, kw in enumerate(secondary_list):
            keywords.append(
                {"keyword": kw, "type": "secondary", "priority": 2 + i}
            )
    return keywords


def call_serpapi(keyword: str, api_key: str):
    """Call SerpAPI with same params as the n8n workflow."""
    url = "https://serpapi.com/search.json"
    params = {
        "q": keyword,
        "location": "United States",
        "google_domain": "google.com",
        "api_key": api_key,
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.RequestException as e:
        return None, str(e)


def extract_paa_answer(q: dict, keyword: str) -> str:
    """Extract answer from a PAA result, handling all formats from n8n logic
    (AI Overview text_blocks, snippet, answer, displayed_answer, list, etc.)."""
    answer = ""

    # AI Overview format
    if q.get("type") == "ai_overview" and q.get("text_blocks"):
        first_block = q["text_blocks"][0]
        if first_block.get("snippet"):
            answer = first_block["snippet"]
        elif isinstance(first_block.get("list"), list):
            answer = ", ".join(first_block["list"])
        elif first_block.get("text"):
            answer = first_block["text"]
    # Standard PAA fallbacks
    elif q.get("snippet") and q["snippet"].strip():
        answer = q["snippet"]
    elif q.get("answer") and q["answer"].strip():
        answer = q["answer"]
    elif q.get("snippet_highlighted_words"):
        answer = " ".join(q["snippet_highlighted_words"])
    elif q.get("displayed_answer") and q["displayed_answer"].strip():
        answer = q["displayed_answer"]
    elif q.get("list"):
        answer = ", ".join(q["list"])
    elif q.get("table"):
        answer = "See detailed comparison table on source page"
    elif q.get("date"):
        answer = q["date"]
    else:
        answer = (
            f"For detailed information about {keyword}, "
            f"consult professional resources."
        )

    # Clean up: strip HTML tags, normalize whitespace
    answer = re.sub(r"<[^>]*>", "", answer)
    answer = re.sub(r"\n+", " ", answer)
    answer = re.sub(r"\s+", " ", answer).strip()
    return answer


def process_search(search_data: dict, metadata: dict):
    """Extract top 5 URLs and all PAA questions from a single SERP response."""
    organic = search_data.get("organic_results") or []
    related = search_data.get("related_questions") or []

    urls = [
        {
            "url": r.get("link", ""),
            "title": r.get("title", ""),
            "snippet": r.get("snippet", ""),
            "keyword": metadata["keyword"],
            "keyword_type": metadata["type"],
            "priority": metadata["priority"],
        }
        for r in organic[:5]
    ]

    paa = []
    for q in related:
        if not q.get("question"):
            continue
        paa.append(
            {
                "question": q.get("question", ""),
                "answer": extract_paa_answer(q, metadata["keyword"]),
                "source": q.get("link", ""),
                "title": q.get("title", ""),
                "keyword": metadata["keyword"],
                "keyword_type": metadata["type"],
                "priority": metadata["priority"],
            }
        )
    return urls, paa


def normalize_question(q: str) -> str:
    """Normalize question text for deduplication
    (lowercase, strip trailing punctuation, normalize whitespace)."""
    return re.sub(r"\s+", " ", re.sub(r"[?!.]+$", "", q.lower().strip()))


def aggregate_paa(all_paa):
    """Aggregate PAA with PRIORITY-FIRST ranking.
    Lower priority number wins; frequency is the tiebreaker."""
    tracker = {}
    for p in all_paa:
        norm = normalize_question(p["question"])
        if not norm:
            continue
        if norm in tracker:
            existing = tracker[norm]
            existing["frequency"] += 1
            existing["keywords"].append(p["keyword"])
            # If this hit is from a higher-priority keyword, take its answer
            if p["priority"] < existing["priority"]:
                existing["priority"] = p["priority"]
                existing["answer"] = p["answer"]
                existing["source"] = p["source"]
                existing["original_question"] = p["question"]
            elif p["priority"] == existing["priority"] and len(p["answer"]) > len(
                existing["answer"]
            ):
                # Same priority — keep longer answer
                existing["answer"] = p["answer"]
        else:
            tracker[norm] = {
                "original_question": p["question"],
                "answer": p["answer"],
                "source": p["source"],
                "frequency": 1,
                "priority": p["priority"],
                "keywords": [p["keyword"]],
            }

    # Priority-first sort: primary keyword PAAs always come before secondary
    sorted_paa = sorted(
        tracker.values(), key=lambda x: (x["priority"], -x["frequency"])
    )[:5]
    return sorted_paa


def aggregate_urls(all_urls):
    """Aggregate URLs with PRIORITY-FIRST ranking.
    Same logic as PAA — primary keyword's URLs come first."""
    tracker = {}
    for u in all_urls:
        if not u["url"]:
            continue
        key = u["url"].lower()
        if key in tracker:
            existing = tracker[key]
            existing["frequency"] += 1
            existing["keywords"].append(u["keyword"])
            if u["priority"] < existing["priority"]:
                existing["priority"] = u["priority"]
        else:
            tracker[key] = {
                "url": u["url"],
                "title": u["title"],
                "snippet": u["snippet"],
                "frequency": 1,
                "priority": u["priority"],
                "keywords": [u["keyword"]],
            }

    sorted_urls = sorted(
        tracker.values(), key=lambda x: (x["priority"], -x["frequency"])
    )[:5]
    return sorted_urls


# -----------------------------
# Run analysis on form submit
# -----------------------------
if submitted:
    if not serpapi_key:
        st.error("Enter your SerpAPI key in the sidebar.")
    elif not report_name.strip():
        st.error("Enter a Report Name.")
    elif not primary_keyword.strip():
        st.error("Enter a Primary Keyword.")
    else:
        keywords = split_keywords(primary_keyword, secondary_keywords)

        all_urls, all_paa = [], []
        progress = st.progress(0, text="Starting…")

        for i, kw in enumerate(keywords):
            progress.progress(
                i / len(keywords),
                text=f"Searching: {kw['keyword']} ({kw['type']})",
            )
            data, error = call_serpapi(kw["keyword"], serpapi_key)
            if error:
                st.warning(f"Error searching '{kw['keyword']}': {error}")
                continue
            urls, paa = process_search(data, kw)
            all_urls.extend(urls)
            all_paa.extend(paa)

        progress.progress(1.0, text="Aggregating…")

        top_paa = aggregate_paa(all_paa)
        top_urls = aggregate_urls(all_urls)

        progress.empty()

        st.success(f"Analysis complete: **{report_name}**")
        st.markdown(
            f"Searched **{len(keywords)}** keyword(s). "
            f"Found **{len(top_paa)}** unique PAA questions and "
            f"**{len(top_urls)}** unique competitor URLs in the top 5."
        )

        col1, col2 = st.columns(2)

        # ---------- PAA box ----------
        with col1:
            st.subheader("Top 5 People Also Ask")
            if top_paa:
                for i, item in enumerate(top_paa, 1):
                    with st.expander(f"**{i}. {item['original_question']}**"):
                        st.write(item["answer"])
                        st.caption(
                            f"From keywords: "
                            f"{', '.join(sorted(set(item['keywords'])))}  "
                            f"·  Frequency: {item['frequency']}  "
                            f"·  Priority: {item['priority']}"
                        )
                        if item["source"]:
                            st.caption(f"Source: {item['source']}")

                paa_df = pd.DataFrame(
                    [
                        {
                            "Rank": i + 1,
                            "Question": p["original_question"],
                            "Answer": p["answer"],
                            "Source URL": p["source"],
                            "Frequency": p["frequency"],
                            "Source Keywords": ", ".join(
                                sorted(set(p["keywords"]))
                            ),
                            "Priority": p["priority"],
                        }
                        for i, p in enumerate(top_paa)
                    ]
                )
                st.download_button(
                    label="Download PAA as CSV",
                    data=paa_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{report_name.replace(' ', '_')}_PAA.csv",
                    mime="text/csv",
                )
            else:
                st.info("No PAA questions found.")

        # ---------- URL box ----------
        with col2:
            st.subheader("Top 5 Competitor URLs")
            if top_urls:
                for i, item in enumerate(top_urls, 1):
                    with st.expander(
                        f"**{i}. {item['title'] or item['url']}**"
                    ):
                        st.markdown(f"**URL:** {item['url']}")
                        st.write(item["snippet"])
                        st.caption(
                            f"Found in: "
                            f"{', '.join(sorted(set(item['keywords'])))}  "
                            f"·  Frequency: {item['frequency']}  "
                            f"·  Priority: {item['priority']}"
                        )

                url_df = pd.DataFrame(
                    [
                        {
                            "Rank": i + 1,
                            "URL": u["url"],
                            "Title": u["title"],
                            "Summary": u["snippet"],
                            "Frequency": u["frequency"],
                            "Found In Keywords": ", ".join(
                                sorted(set(u["keywords"]))
                            ),
                            "Priority": u["priority"],
                        }
                        for i, u in enumerate(top_urls)
                    ]
                )
                st.download_button(
                    label="Download URLs as CSV",
                    data=url_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{report_name.replace(' ', '_')}_URLs.csv",
                    mime="text/csv",
                )
            else:
                st.info("No competitor URLs found.")
