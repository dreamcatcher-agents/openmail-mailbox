from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _adapter_class() -> ast.ClassDef:
    tree = ast.parse((ROOT / "adapter.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "OpenMailMailboxAdapter":
            return node
    raise AssertionError("OpenMailMailboxAdapter class not found")


def test_connect_accepts_keyword_only_is_reconnect_with_false_default() -> None:
    connect = next(
        node
        for node in _adapter_class().body
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == "connect"
    )
    names = [arg.arg for arg in connect.args.kwonlyargs]
    assert "is_reconnect" in names
    index = names.index("is_reconnect")
    default = connect.args.kw_defaults[index]
    assert isinstance(default, ast.Constant)
    assert default.value is False


def test_manifest_version_tracks_platform_contract_update() -> None:
    manifest = (ROOT / "plugin.yaml").read_text(encoding="utf-8")
    assert "version: 0.1.1" in manifest
