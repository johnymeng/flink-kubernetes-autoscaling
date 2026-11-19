"""Identify the source and join vertices in the submitted Q8 job graph."""
import re


def topology(job):
    vertices = {vertex["id"]: vertex for vertex in (job or {}).get("vertices", [])}
    nodes = {node["id"]: node for node in ((job or {}).get("plan") or {}).get("nodes", [])}
    sources = [vertex for vid, vertex in vertices.items()
               if vertex["name"].lower().startswith("source:") or (vid in nodes and not nodes[vid].get("inputs"))]
    labels = {vid: vertex["name"] + " " + nodes.get(vid, {}).get("description", "") for vid, vertex in vertices.items()}
    named = [vid for vid, label in labels.items() if re.search(r"\b(?:WindowJoin|Join)\s*[\[(]", label, re.I)]
    if not named:
        source_ids = {s["id"] for s in sources}
        named = [vid for vid, label in labels.items() if vid not in source_ids and re.search(r"join|co[ -]?group", label, re.I)]
    two_inputs = [vid for vid, node in nodes.items() if vid in vertices and len(node.get("inputs", ())) == 2]
    candidates = [vid for vid in named if vid in two_inputs] or named or two_inputs
    return sources, candidates[0] if len(candidates) == 1 else None
