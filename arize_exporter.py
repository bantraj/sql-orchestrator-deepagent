import os
import requests
import pandas as pd

PHOENIX_BASE_URL = os.getenv("PHOENIX_BASE_URL")
SPACE_ID = os.getenv("PHOENIX_SPACE_ID")
API_KEY = os.getenv("PHOENIX_API_KEY")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

GRAPHQL_QUERY = """
query GetSpans($spaceId: ID!, $limit: Int!) {
  spans(
    spaceId: $spaceId
    first: $limit
  ) {
    edges {
      node {
        traceId
        spanId
        parentSpanId
        name
        startTime
        endTime
        latencyMs
        statusCode
        attributes
      }
    }
  }
}
"""

def fetch_spans(limit=1000):
    url = f"{PHOENIX_BASE_URL}/graphql"

    payload = {
        "query": GRAPHQL_QUERY,
        "variables": {
            "spaceId": SPACE_ID,
            "limit": limit
        }
    }

    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()

    return response.json()


def flatten_spans(response):
    rows = []

    edges = response["data"]["spans"]["edges"]
    for edge in edges:
        span = edge["node"]
        rows.append({
            "trace_id": span["traceId"],
            "span_id": span["spanId"],
            "parent_span_id": span["parentSpanId"],
            "name": span["name"],
            "start_time": span["startTime"],
            "end_time": span["endTime"],
            "latency_ms": span["latencyMs"],
            "status": span["statusCode"],
            "attributes": span["attributes"],
        })

    return rows

def export_to_csv(rows, file_name="phoenix_traces.csv"):
    df = pd.DataFrame(rows)
    df.to_csv(file_name, index=False)
    print(f"✅ Exported {len(df)} spans to {file_name}")


if __name__ == "__main__":
    response = fetch_spans(limit=1000)
    rows = flatten_spans(response)
    export_to_csv(rows)
