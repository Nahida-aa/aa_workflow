#!/usr/bin/env python3
"""Generate markdown API reference from rustdoc JSON.

Mirrors the layout of the upstream TanStack Workflow docs/reference/
(typedoc-plugin-markdown output): one file per item, frontmatter
`id`/`title`, `index.md` with per-kind sections.

Usage:
    python3 scripts/generate-docs.py [--no-build]

Requires nightly for rustdoc JSON (`rustup run nightly cargo rustdoc ...`).
Output: docs/reference/ (committed, like upstream).
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CRATE = "aa-workflow-core"
CRATE_SLUG = "aa-workflow-core"
JSON_PATH = REPO / "target" / "doc" / f"{CRATE.replace('-', '_')}.json"
OUT = REPO / "docs" / "reference"

# inner kind -> (dirname, page title prefix)
KIND_DIRS = {
    "struct": ("structs", "Struct"),
    "enum": ("enums", "Enum"),
    "trait": ("traits", "Trait"),
    "function": ("functions", "Function"),
    "type_alias": ("type-aliases", "Type Alias"),
    "constant": ("constants", "Constant"),
    "static": ("constants", "Static"),
}


def build_json(no_build: bool) -> None:
    if no_build and JSON_PATH.exists():
        return
    cargo = shutil.which("cargo") or "cargo"
    subprocess.run(
        [
            "rustup", "run", "nightly", cargo, "rustdoc",
            "-p", CRATE, "--", "-Z", "unstable-options",
            "--output-format", "json",
        ],
        cwd=REPO, check=True,
    )


def last_seg(path: str) -> str:
    return path.rsplit("::", 1)[-1]


def render_type(t: dict | None) -> str:
    """Best-effort signature rendering from a rustdoc-json type tree."""
    if t is None:
        return "()"
    if "generic" in t:
        return t["generic"]
    if "primitive" in t:
        return t["primitive"]
    if "resolved_path" in t:
        rp = t["resolved_path"]
        s = last_seg(rp["path"])
        args = rp.get("args")
        if args and "angle_bracketed" in args:
            parts = []
            for a in args["angle_bracketed"].get("args") or []:
                if "type" in a:
                    parts.append(render_type(a["type"]))
                elif "lifetime" in a:
                    lt = a["lifetime"]
                    parts.append(lt if isinstance(lt, str) else (lt.get("args") or ["'_"])[0] if isinstance(lt.get("args"), list) and lt.get("args") else "'_")
            if parts:
                s += "<" + ", ".join(parts) + ">"
        return s
    if "borrowed_ref" in t:
        br = t["borrowed_ref"]
        mut = "mut " if br.get("mutable") else ""
        lt = br.get("lifetime") or ""
        return f"&{lt + ' ' if lt else ''}{mut}{render_type(br['type'])}"
    if "tuple" in t:
        types = t["tuple"]
        return "()" if not types else "(" + ", ".join(render_type(x) for x in types) + ")"
    if "slice" in t:
        s = t["slice"]
        # format 60：{"slice": <type>}；旧版 {"slice": {"type": ...}}
        return f"[{render_type(s.get('type') if isinstance(s, dict) and 'type' in s else s)}]"
    if "array" in t:
        a = t["array"]
        elem = a.get("type") if isinstance(a, dict) and "type" in a else a
        return f"[{render_type(elem)}; {a.get('len', 'N') if isinstance(a, dict) else 'N'}]"
    if "dyn_trait" in t:
        trs = " + ".join(
            last_seg(x["trait"]["path"]) + render_args_paren(x["trait"].get("args"))
            for x in t["dyn_trait"]["traits"]
        )
        return f"dyn {trs}"
    if "impl_trait" in t:
        # format 60 里 impl_trait 直接是 bounds 数组（旧版是 {"trait_bounds": [...]}）
        bounds = t["impl_trait"] if isinstance(t["impl_trait"], list) else t["impl_trait"]["trait_bounds"]
        return "impl " + " + ".join(
            last_seg(b["trait"]["path"]) if isinstance(b, dict) and "trait" in b else "?"
            for b in bounds
        )
    if "function_pointer" in t:
        return render_fn_sig("fn", t["function_pointer"]["sig"], None)
    if "qualified_path" in t:
        return t["qualified_path"].get("name", "?")
    if "unnamed" in t:
        return "_"
    return "?"  # 未知形态的兜底


def render_args_paren(args) -> str:
    """Parenthesized args（Fn(A) -> B 形态）。"""
    if not args or "parenthesized" not in args:
        return ""
    p = args["parenthesized"]
    ins = ", ".join(render_type(x) for x in p.get("inputs") or [])
    out = p.get("output")
    return f"({ins})" + (f" -> {render_type(out)}" if out else "")


def render_generics(g: dict) -> tuple[str, str]:
    """返回 (<>内参数, where 子句) 的 best-effort 渲染。"""
    params = []
    for p in g.get("params") or []:
        k = p.get("kind") or ("generic" if "generic" in p else None)
        if isinstance(k, dict) and "lifetime" in str(k):
            pass
        name = p.get("name") or ""
        if "lifetime" in (p.get("kind") or {}):
            params.append(name)
            continue
        bounds = render_bounds(p.get("bounds") or [])
        params.append(f"{name}: {bounds}" if bounds else name)
    wheres = []
    for wp in g.get("where_predicates") or []:
        b = wp.get("bound") or {}
        if "trait_bound" in b:
            tb = b["trait_bound"]
            tr = tb["trait"]["path"] if "trait" in tb else "?"
            wheres.append(f"{wp.get('path', '?')}: {last_seg(tr)}")
        elif "outlives" in b:
            wheres.append(f"{b['outlives']}: '_")
    gen = f"<{', '.join(params)}>" if params else ""
    where = f"\nwhere\n    {',\n    '.join(wheres)}\n" if wheres else ""
    return gen, where


def render_bounds(bounds: list) -> str:
    parts = []
    for b in bounds:
        if not isinstance(b, dict):
            parts.append(str(b))
        elif "trait_bound" in b:
            tb = b["trait_bound"]
            parts.append(last_seg(tb["trait"]["path"]) + render_args_paren(tb["trait"].get("args")))
        elif "trait" in b:
            parts.append(last_seg(b["trait"]["path"]))
        elif "outlives" in b:
            parts.append(b["outlives"])
    return " + ".join(parts)


def render_fn_sig(name: str, sig: dict, header: dict | None) -> str:
    h = header or {}
    kw = ""
    if h.get("is_const"):
        kw += "const "
    if h.get("is_async"):
        kw += "async "
    if h.get("is_unsafe"):
        kw += "unsafe "
    inputs = sig.get("inputs") or []
    rendered = []
    for i, (n, t) in enumerate(inputs):
        if i == 0 and n == "self":
            # 折回惯用 self 形态：&Self → &self，&mut Self → &mut self
            if isinstance(t, dict) and "borrowed_ref" in t:
                br = t["borrowed_ref"]
                if isinstance(br.get("type"), dict) and br["type"].get("generic") == "Self":
                    rendered.append("&mut self" if br.get("mutable") else "&self")
                    continue
            if isinstance(t, dict) and t.get("generic") == "Self":
                rendered.append("self")
                continue
        rendered.append(f"{n}: {render_type(t)}")
    args = ", ".join(rendered)
    out = sig.get("output")
    ret = f" -> {render_type(out)}" if out else ""
    return f"{kw}fn {name}({args}){ret}"


class Docs:
    def __init__(self, data: dict):
        self.idx: dict[int, dict] = {int(k): v for k, v in data["index"].items()}
        self.root_id = int(data["root"])

    def collect_public(self) -> dict[str, list[dict]]:
        """从 crate root 递归收集 public items（含 re-export，按 id 去重）。"""
        found: dict[str, dict[int, dict]] = defaultdict(dict)
        seen: set[int] = set()

        def walk(module_id: int) -> None:
            mod = self.idx[module_id]["inner"]["module"]
            for iid in mod.get("items") or []:
                if iid in seen:
                    continue
                seen.add(iid)
                it = self.idx[iid]
                if it["visibility"] != "public":
                    continue
                inner = it["inner"]
                if "use" in inner:
                    # pub use 再导出：跟随目标 id 分类注册（glob/外部目标跳过）。
                    # 以目标 id 为 key——若该 item 也能经 pub module 走到，两路会
                    # 落到同一个 key 上，天然去重。
                    u = inner["use"]
                    if u.get("is_glob") or u.get("id") is None or u["id"] not in self.idx:
                        continue
                    tgt = self.idx[u["id"]]
                    for kind in KIND_DIRS:
                        if kind in tgt["inner"]:
                            found[kind][u["id"]] = tgt
                            break
                    continue
                for kind in KIND_DIRS:
                    if kind in inner:
                        found[kind][iid] = it
                        break
                else:
                    if "module" in inner:
                        walk(iid)

        walk(self.root_id)
        return {k: sorted(v.values(), key=lambda x: x["name"] or "") for k, v in found.items()}


def frontmatter(name: str) -> str:
    return f"---\nid: {name}\ntitle: {name}\n---\n\n"


def defined_in(it: dict) -> str:
    sp = it.get("span")
    if not sp:
        return ""
    return f"Defined in: `{sp['filename']}:{sp['begin'][0]}`\n"


def write_page(path: Path, name: str, title: str, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(frontmatter(name) + f"# {title}\n\n" + body, encoding="utf-8")


def methods_of(docs: Docs, it: dict) -> tuple[list[dict], list[str]]:
    """返回 (固有方法 items, 非合成 trait impl 名列表, 合成 trait 名列表)。"""
    inner = it["inner"].get("struct") or it["inner"].get("enum") or {}
    methods, trait_impls, synthetic = [], [], []
    for iid in inner.get("impls") or []:
        imp = docs.idx[iid]["inner"]["impl"]
        if imp.get("is_negative"):
            continue
        if imp.get("trait") is None:
            for mid in imp.get("items") or []:
                m = docs.idx[mid]
                if "function" in m["inner"] and m["visibility"] == "public":
                    methods.append(m)
        else:
            tname = last_seg(imp["trait"]["path"])
            if imp.get("is_synthetic"):
                synthetic.append(tname)
            else:
                trait_impls.append(tname)
    return methods, trait_impls, synthetic


def fn_section(docs: Docs, it: dict, level: str = "###") -> str:
    f = it["inner"]["function"]
    gen, where = render_generics(f.get("generics") or {"params": [], "where_predicates": []})
    sig = render_fn_sig(it["name"] or "_", f["sig"], f.get("header"))
    # 把 <gen> 和 where 塞进签名
    if gen:
        sig = sig.replace(f"fn {it['name']}(", f"fn {it['name']}{gen}(", 1)
    if where:
        sig = sig + where.rstrip("\n")
    out = f"{level} `{it['name']}`\n\n```rust\npub {sig}\n```\n\n"
    if it.get("docs"):
        out += it["docs"].rstrip() + "\n\n"
    di = defined_in(it)
    if di:
        out += di + "\n"
    return out


def render_item_page(docs: Docs, kind: str, it: dict) -> str:
    _, prefix = KIND_DIRS[kind]
    body = defined_in(it) + "\n"
    if it.get("docs"):
        body += it["docs"].rstrip() + "\n\n"
    inner = it["inner"][kind]

    if kind == "struct":
        sk = inner.get("kind") or {}
        if "plain" in sk:
            fields = sk["plain"].get("fields") or []
            if fields:
                body += "## Fields\n\n"
                for fid in fields:
                    fl = docs.idx[fid]
                    sf = fl["inner"].get("struct_field")
                    # format 60：struct_field 直接是类型；旧版是 {"type": ...}
                    ft = sf.get("type") if isinstance(sf, dict) and "type" in sf else sf
                    body += f"### `{fl['name']}`\n\n```rust\n{fl['name']}: {render_type(ft)}\n```\n\n"
                    if fl.get("docs"):
                        body += fl["docs"].rstrip() + "\n\n"
            if sk["plain"].get("has_stripped_fields"):
                body += "_（存在非公开字段）_\n\n"
        methods, trait_impls, synthetic = methods_of(docs, it)

    elif kind == "enum":
        variants = inner.get("variants") or []
        if variants:
            body += "## Variants\n\n"
            for vid in variants:
                v = docs.idx[vid]
                body += f"### `{v['name']}`\n\n"
                vk = v["inner"].get("variant", {}).get("kind") or {}
                if "tuple" in vk:
                    fields = vk["tuple"] if isinstance(vk["tuple"], list) else vk["tuple"].get("fields") or []
                    tys = []
                    for x in fields:
                        # format 60：tuple 字段是 item id，且 struct_field 直接是类型
                        if isinstance(x, int):
                            sf = docs.idx[x]["inner"].get("struct_field")
                            tys.append(render_type(sf.get("type") if isinstance(sf, dict) and "type" in sf else sf))
                        else:
                            tys.append(render_type(x))
                    body += "```rust\n(" + ", ".join(tys) + ")\n```\n\n"
                elif "struct" in vk:
                    body += "```rust\n{ .. }\n```\n\n"
                if v.get("docs"):
                    body += v["docs"].rstrip() + "\n\n"
        methods, trait_impls, synthetic = methods_of(docs, it)

    elif kind == "trait":
        methods = []
        required, provided = [], []
        for iid in inner.get("items") or []:
            m = docs.idx[iid]
            if "function" not in m["inner"] or m["visibility"] != "public":
                continue
            f = m["inner"]["function"]
            (provided if f.get("has_body") else required).append(m)
        if required:
            body += "## Required Methods\n\n"
            body += "".join(fn_section(docs, m) for m in required)
        if provided:
            body += "## Provided Methods\n\n"
            body += "".join(fn_section(docs, m) for m in provided)
        trait_impls, synthetic = [], []

    else:  # function / type_alias / constant / static
        if kind == "function":
            gen, where = render_generics(inner.get("generics") or {"params": [], "where_predicates": []})
            sig = render_fn_sig(it["name"] or "_", inner["sig"], inner.get("header"))
            if gen:
                sig = sig.replace(f"fn {it['name']}(", f"fn {it['name']}{gen}(", 1)
            if where:
                sig += where.rstrip("\n")
            body += "## Signature\n\n```rust\npub " + sig + "\n```\n\n"
            return body
        if kind == "type_alias":
            body += "## Definition\n\n```rust\npub type " + (it["name"] or "?") + " = " + render_type(inner.get("type")) + "\n```\n\n"
        if kind in ("constant", "static"):
            kw = "const" if kind == "constant" else "static"
            body += "## Definition\n\n```rust\npub " + kw + " " + (it["name"] or "?") + ": " + render_type(inner.get("type"))
            if inner.get("value") is not None:
                body += " = " + json.dumps(inner["value"])
            body += "\n```\n\n"
        methods, trait_impls, synthetic = [], [], []

    if kind in ("struct", "enum"):
        if methods:
            body += "## Implementations\n\n" + "".join(fn_section(docs, m) for m in methods)
        if trait_impls:
            body += "## Trait Implementations\n\n"
            body += "".join(f"- `impl {t} for {it['name']}`\n" for t in trait_impls)
            body += "\n"
        if synthetic:
            body += "## Auto Trait Implementations\n\n"
            body += " ".join(f"`{t}`" for t in sorted(set(synthetic))) + "\n\n"
    return body


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-build", action="store_true", help="复用已有 target/doc/*.json")
    args = ap.parse_args()
    build_json(args.no_build)

    data = json.loads(JSON_PATH.read_text())
    docs = Docs(data)
    found = docs.collect_public()

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)

    index_sections = []
    for kind in ("struct", "enum", "trait", "function", "type_alias", "constant", "static"):
        items = found.get(kind) or []
        if not items:
            continue
        dirname, prefix = KIND_DIRS[kind]
        plural = {"struct": "Structs", "enum": "Enums", "trait": "Traits", "function": "Functions",
                  "type_alias": "Type Aliases", "constant": "Constants", "static": "Statics"}[kind]
        section = f"## {plural}\n\n"
        for it in items:
            page = render_item_page(docs, kind, it)
            write_page(OUT / dirname / f"{it['name']}.md", it["name"], f"{prefix}: {it['name']}", page)
            section += f"- [{it['name']}]({dirname}/{it['name']}.md)\n"
        index_sections.append(section)

    (OUT / "index.md").write_text(
        frontmatter(CRATE_SLUG)
        + f"# {CRATE_SLUG}\n\n"
        + "".join(index_sections),
        encoding="utf-8",
    )

    total = sum(len(v) for v in found.values())
    print(f"生成完毕：{total} 个 item → {OUT}")
    for kind in found:
        print(f"  {kind}: {len(found[kind])}")


if __name__ == "__main__":
    sys.exit(main())
