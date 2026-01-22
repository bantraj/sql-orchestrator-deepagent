headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

graphql_query = """
query TraceSlideOverQuery(
  $modelId: ID!
  $dataset: ModelDatasetInput!
  $maxNumRecords: Int!
) {
  traceSlideOverQuery(
    modelId: $modelId
    dataset: $dataset
    maxNumRecords: $maxNumRecords
  ) {
    edges {
      span {
        traceId
        spanId
        parentSpanId
        name
        startTime
        endTime
        latencyMs
        attributes
      }
    }
  }
}
"""


variables = {
    "modelId": "W9kZMw6MTENjNMWTNk6ODpRw",
    "dataset": {
        "startTime": "2026-01-17T18:30:00.000Z",
        "endTime": "2026-01-21T18:29:59.999Z",
        "environmentName": "tracing",
        "filters": [],
        "queryFilter": {
            "context.trace_id": "0add94cc2bdb571ab140e806de5abf1"
        }
    },
    "maxNumRecords": 6000
}

payload = {
    "operationName": "TraceSlideOverQuery",
    "query": graphql_query,
    "variables": variables
}


import requests

response = requests.post(
    f"{PHOENIX_BASE_URL}/graphql",
    headers=headers,
    json=payload,
    timeout=60
)

print(response.status_code)
print(response.text)
response.raise_for_status()


import json
print(json.dumps(payload, indent=2))
