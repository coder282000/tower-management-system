"""Correlation service (manager tier): turns raw sensor readings into fault incidents.

Moved out of nodes/node.py in Milestone 3 with no change in behaviour. Kept separate
so it can be swapped for a richer rule set later, and so the edge-vs-core experiment
can run the same logic at a different tier.
"""
import time


class CorrelationService:
    def __init__(self, node):
        self.node = node

    def correlate(self, site, data):
        """A power fault (grid or link down) opens an incident; recovery closes it.
        Returns ("OPEN", incident), ("RESOLVED", {"site": site}) or (None, None)."""
        node = self.node
        fault = (not data.get("grid", True)) or (not data.get("link", True))
        open_incident = node.incidents.get(site)
        if fault and not open_incident:
            incident = {"site": site, "cause": "grid" if not data.get("grid", True) else "link",
                        "opened": time.time(), "battery": data.get("battery")}
            if data.get("fid"):
                incident["fid"] = data["fid"]       # fault id, only present when the run traces faults (Milestone 8)
            node.incidents[site] = incident
            node.lc += 1        # local event ticks the clock
            node.log.event("correlated_incident", site=site, cause=incident["cause"], fid=incident.get("fid"), lc=node.lc)
            return ("OPEN", incident)
        if not fault and open_incident:
            del node.incidents[site]
            node.log.event("incident_resolved", site=site,
                           duration=time.time() - open_incident["opened"])
            return ("RESOLVED", {"site": site})
        return (None, None)

    def stateless(self, site, data, is_open):
        """Milestone 8 processor-pool model: the pool manager keeps no state. The core owns the incident table and
        tells the manager whether this site's incident is already open."""
        fault = (not data.get("grid", True)) or (not data.get("link", True))
        if fault and not is_open:
            inc = {"site": site, "cause": "grid" if not data.get("grid", True) else "link",
                   "opened": time.time(), "battery": data.get("battery")}
            if data.get("fid"):
                inc["fid"] = data["fid"]
            return ("OPEN", inc)
        if not fault and is_open:
            return ("RESOLVED", {"site": site})
        return (None, None)
