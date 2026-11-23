"""Line-level release provenance for the five drift modules."""

import ast
from collections import Counter
from difflib import SequenceMatcher, unified_diff
from hashlib import sha256
import json
from pathlib import Path
import re


REVIEW_PATH = Path(__file__).with_name("source-provenance.json")
MODULES = ("cloud_guard", "cloud_probe", "dhan_broker", "risk_manager", "survivor_engine")


def normalized(raw):
    return raw.replace(b"\r\n", b"\n")


def digest(raw):
    return sha256(raw).hexdigest()


def inventory(raw):
    """Return qualified functions and the residual module/class source."""
    source = normalized(raw)
    lines = source.decode("utf-8").splitlines(keepends=True)
    locations = {}
    module_lines = dict(enumerate(lines, 1))

    class Visitor(ast.NodeVisitor):
        scope = ()
        counts = Counter()

        def descend(self, node):
            old = self.scope
            self.scope = (*old, node.name)
            self.generic_visit(node)
            self.scope = old

        visit_ClassDef = descend

        def visit_FunctionDef(self, node):
            qualified = ".".join((*self.scope, node.name))
            self.counts[qualified] += 1
            occurrence = self.counts[qualified]
            name = qualified if occurrence == 1 else f"{qualified}#{occurrence}"
            first = min([node.lineno, *(item.lineno for item in node.decorator_list)])
            locations[name] = (first, node.end_lineno)
            self.descend(node)

        visit_AsyncFunctionDef = visit_FunctionDef

    Visitor().visit(ast.parse(source))
    result = {}
    for name, (first, last) in locations.items():
        result[name] = {line: lines[line - 1] for line in range(first, last + 1)}
        for line in result[name]:
            module_lines.pop(line, None)
    result["<module>"] = module_lines
    return result


def scope_sha(lines):
    return digest("".join(lines.values()).encode("utf-8")) if lines is not None else None


def edits(before, after):
    old, new = list(before.items()), list(after.items())
    matcher = SequenceMatcher(None, list(before.values()), list(after.values()), autojunk=False)
    return [
        {"operation": tag, "deployed": old[i:j], "candidate": new[k:l]}
        for tag, i, j, k, l in matcher.get_opcodes() if tag != "equal"
    ]


def covered(references, catalog, payload):
    qa = [catalog[reference] for reference in references]
    return bool(qa) and all(
        item["path"] in payload
        and digest(payload[item["path"]]) == item["sha256"]
        and re.search(
            rb"(?m)^\s*def " + re.escape(item["test"].encode()) + rb"\(",
            payload[item["path"]],
        )
        for item in qa
    )


def review_module(module, before, after, review, qa_catalog, payload):
    """The committed ledger grants approval only for the pinned source and QA."""
    old, new = inventory(before), inventory(after)
    pair = {"deployed": digest(before), "candidate": digest(after)}
    pinned = pair == review.get("source_sha256")
    retired = module == "risk_manager"
    rows = {}
    for name in sorted(old.keys() | new.keys()):
        a, b = old.get(name), new.get(name)
        fingerprints = {"deployed": scope_sha(a), "candidate": scope_sha(b)}
        row = rows[name] = {
            "sha256": fingerprints,
            "source_lines": {"deployed": [min(a), max(a)] if a else None,
                             "candidate": [min(b), max(b)] if b else None},
            "classification": "identical",
        }
        if fingerprints["deployed"] == fingerprints["candidate"]:
            continue
        entry = review.get("scopes", {}).get(name, {})
        matched = pinned and fingerprints == entry.get("sha256")
        row.update(classification="unreviewed", edits=edits(a or {}, b or {}))
        if not matched:
            row["reason"] = "No provenance entry for this exact source pair and scope"
            continue
        row["provenance"] = entry
        for patch in entry.get("approved_subdeltas", []):
            row.setdefault("subdelta_review", []).append({
                **patch,
                "classification": "intentional" if covered(patch["qa"], qa_catalog, payload) else "unreviewed",
            })
        if retired and entry.get("decision") == "retired_excluded":
            row.update(classification="retired_excluded", reason=entry["reason"])
        elif entry.get("decision") == "intentional" and covered(entry["qa"], qa_catalog, payload):
            row.update(classification="intentional", reason=entry["reason"])
        elif entry.get("decision") == "intentional":
            row["reason"] = "Pinned QA assertions missing from the exported test source"
        else:
            row["reason"] = entry["reason"]
    return {
        "source_sha256": pair,
        "matches_ledger": pinned,
        "normalized_sha256": {"deployed": digest(normalized(before)),
                              "candidate": digest(normalized(after))},
        "lifecycle": "retired_excluded" if retired else "reviewed_for_parity",
        "inventory": rows,
        "normalized_diff": list(unified_diff(
            normalized(before).decode("utf-8").splitlines(keepends=True),
            normalized(after).decode("utf-8").splitlines(keepends=True),
            fromfile="deployed", tofile="candidate",
        )),
    }


def review_sources(candidate, baseline, payload):
    ledger = json.loads(REVIEW_PATH.read_bytes())
    records = {}
    for module in MODULES:
        name = f"app/{module}.py"
        records[name] = review_module(
            module, baseline[name], candidate[name], ledger["modules"].get(name, {}),
            ledger["qa"], payload,
        )
    return {
        "schema_version": ledger["schema_version"],
        "ledger_sha256": digest(REVIEW_PATH.read_bytes()),
        "sources": ledger["sources"],
        "qa": ledger["qa"],
        "modules": records,
    }
