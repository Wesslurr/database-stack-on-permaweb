import sys
sys.path.insert(0, '/home/weslur/database')
from tools.client import StateEngineClient
c = StateEngineClient()
p = c.read_all_points()
print("Total:", p.get("total"))
print("ActiveEntityCount:", p.get("active_entity_count"))
print("Points:", [x.get("id") for x in p.get("points", [])])
