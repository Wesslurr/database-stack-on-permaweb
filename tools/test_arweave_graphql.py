import urllib.request
import json

q = {
    "query": """{
  transactions(tags: [{ name: "App-Name", values: ["HyperStack"] }]) {
    edges {
      node {
        id
        tags {
          name
          value
        }
      }
    }
  }
}"""
}
req = urllib.request.Request(
    'http://127.0.0.1:1984/graphql',
    data=json.dumps(q).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)
resp = urllib.request.urlopen(req).read().decode('utf-8')
print("GraphQL response:", resp)
