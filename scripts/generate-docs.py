#!/usr/bin/env python3
"""Generate markdown API reference from rustdoc JSON.

Mirrors the layout of the upstream TanStack Workflow docs/reference/
(typedoc-plugin-markdown output): one file per item, frontmatter
`id`/`title`, `index.md` with per-kind sections, and per-item
Type Parameters / Parameters / Returns sections with cross-item links.

Usage:
    python3 scripts/generate-docs.py [--no-build]

Requires nightly for rustdoc JSON (`rustup run nightly cargo rustdoc ...`).
Output: docs/reference/ (committed, like upstream).
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CRATE = "aa_workflow_core"
CRATE_SLUG = "aa_workflow_core"
JSON_PATH = REPO / "target" / "doc" / f"{CRATE.replace('-', '_')}.json"
OUT = REPO / "docs" / "reference"

EMPTY_G = {"params": [], "where_predicates": []}

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

# item id -> (dirname, name)，渲染期填充
LINK_TARGETS: dict[int, tuple[str, str]] = {}


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


def sub_level(level: str) -> str:
    return "#" + level


def resolve_link(tgt, here: str) -> str | None:
    """item id → 相对本页的 markdown 链接；解析不了返回 None。"""
    hit = LINK_TARGETS.get(tgt) if isinstance(tgt, int) else None
    if not hit:
        return None
    d, name = hit
    if d == here:
        return f"{name}.md"
    return f"{d}/{name}.md" if not here else f"../{d}/{name}.md"


def render_type(t) -> str:
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
                    parts.append(lt if isinstance(lt, str)
                                 else ((lt.get("args") or ["'_"])[0]
                                       if isinstance(lt.get("args"), list) and lt.get("args") else "'_"))
            if parts:
                s += "<" + ", ".join(parts) + ">"
        return s
    if "borrowed_ref" in t:
        br = t["borrowed_ref"]
        mut = "mut " if br.get("mutable") else ""
        lt = br.get("lifetime") or ""
        return f"&{lt + ' ' if lt else ''}{mut}{render_type(br['type'])}"
    if "tuple" in t:
        types = t["tuple"] if isinstance(t["tuple"], list) else t["tuple"].get("types") or []
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
            render_type({"resolved_path": b["trait"]}) if isinstance(b, dict) and "trait" in b else "?"
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


def render_type_linked(t, here: str) -> str:
    """Section（代码块外）用：可解析到本 reference 页的类型渲染成链接。"""
    if isinstance(t, dict) and "resolved_path" in t:
        tgt = resolve_link(t["resolved_path"].get("id"), here)
        if tgt:
            return f"[`{render_type(t)}`]({tgt})"
    return f"`{render_type(t)}`"


def render_bounds_linked(bounds: list, here: str) -> str:
    parts = []
    for b in bounds or []:
        if not isinstance(b, dict):
            parts.append(str(b))
            continue
        tb = b.get("trait_bound") or (b if "trait" in b else None)
        if tb and "trait" in tb:
            tr = tb["trait"]
            txt = last_seg(tr["path"]) + render_args_paren(tr.get("args"))
            tgt = resolve_link(tr.get("id"), here)
            parts.append(f"[`{txt}`]({tgt})" if tgt else f"`{txt}`")
        elif "outlives" in b:
            parts.append(b["outlives"])
    return " + ".join(parts)


def render_generics(g: dict) -> tuple[str, str]:
    """返回 (<>内参数, where 子句) 的 best-effort 渲染（代码块内纯文本）。"""
    params = []
    for p in g.get("params") or []:
        k = p.get("kind") or {}
        name = p.get("name") or ""
        if "lifetime" in k:
            params.append(name)
            continue
        bounds = render_bounds_linked(k.get("type", {}).get("bounds") if "type" in k else [], "")
        bounds = bounds.replace("`", "")
        params.append(f"{name}: {bounds}" if bounds else name)
    wheres = []
    for wp in g.get("where_predicates") or []:
        bp = wp.get("bound_predicate") or wp.get("bound") or {}
        ty = render_type(bp.get("type"))
        bs = render_bounds_linked(bp.get("bounds") or [], "").replace("`", "")
        if bs:
            wheres.append(f"{ty}: {bs}")
    gen = f"<{', '.join(params)}>" if params else ""
    where = f"\nwhere\n    {',\n    '.join(wheres)}\n" if wheres else ""
    return gen, where


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


def non_self_inputs(sig: dict) -> list:
    return [(n, t) for i, (n, t) in enumerate(sig.get("inputs") or [])
            if not (i == 0 and n == "self")]


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


def build_link_targets(docs: Docs, found: dict[str, list[dict]]) -> None:
    """item id → (dirname, name)。方法/变体等子 item 指到父页面。"""
    for kind, items in found.items():
        d = KIND_DIRS[kind][0]
        for it in items:
            LINK_TARGETS[it["id"]] = (d, it["name"])
            inner = it["inner"][kind]
            if kind in ("struct", "enum"):
                for iid in inner.get("impls") or []:
                    imp = docs.idx[iid]["inner"]["impl"]
                    for mid in imp.get("items") or []:
                        m = docs.idx[mid]
                        if "function" in m["inner"]:
                            LINK_TARGETS[m["id"]] = (d, it["name"])
                if kind == "enum":
                    for vid in inner.get("variants") or []:
                        LINK_TARGETS[vid] = (d, it["name"])
            elif kind == "trait":
                for iid in inner.get("items") or []:
                    LINK_TARGETS[iid] = (d, it["name"])


def expand_docs(it: dict, here: str) -> str:
    """把 doc 注释里的 rustdoc intra-doc link 展开成相对 markdown 链接。

    links 字典的 key 有两种存法（value 都是目标 item id）：
    - shortcut 形式 `[`Foo`]`：key 就是 "`Foo`"（含反引号）
    - 带路径形式 `[`text`](Foo::bar)`：key 是路径部分 "Foo::bar"
    解析不了的目标退化为纯 code 文本（去掉方括号/链接）。
    """
    text = it.get("docs")
    if not text:
        return ""
    for key, tgt in (it.get("links") or {}).items():
        target = resolve_link(tgt, here)
        esc = re.escape(key)
        if target:
            # 带路径形式：保留原文本，换 url
            text = re.sub(r"\[([^\]]+)\]\(" + esc + r"\)",
                          lambda m: f"[{m.group(1)}]({target})", text)
            # shortcut 形式
            text = text.replace("[" + key + "]", f"[{key}]({target})")
        else:
            text = re.sub(r"\[([^\]]+)\]\(" + esc + r"\)", lambda m: m.group(1), text)
            text = text.replace("[" + key + "]", key)
    return text.rstrip() + "\n\n"


def defined_in(it: dict, here: str) -> str:
    sp = it.get("span")
    if not sp:
        return ""
    fname, line = sp["filename"], sp["begin"][0]
    prefix = "../../" if not here else "../../../"
    return f"Defined in: [`{fname}:{line}`]({prefix}{fname}#L{line})\n"


def write_page(path: Path, name: str, title: str, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\nid: {name}\ntitle: {name}\n---\n\n# {title}\n\n{body}", encoding="utf-8")


def methods_of(docs: Docs, it: dict) -> tuple[list[dict], list[str], list[str]]:
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


def fn_section(docs: Docs, it: dict, here: str, level: str = "###") -> str:
    """方法/函数条目：标题 + 签名 + Defined in + docs + Parameters + Returns。"""
    f = it["inner"]["function"]
    name = it["name"] or "_"
    gen, where = render_generics(f.get("generics") or EMPTY_G)
    sig = render_fn_sig(name, f["sig"], f.get("header"))
    if gen:
        sig = sig.replace(f"fn {name}(", f"fn {name}{gen}(", 1)
    if where:
        sig += where.rstrip("\n")
    out = f"{level} {name}()\n\n```rust\npub {sig}\n```\n\n"
    di = defined_in(it, here)
    if di:
        out += di + "\n"
    out += expand_docs(it, here)
    sub, entry = sub_level(level), sub_level(sub_level(level))
    params = non_self_inputs(f["sig"])
    if params:
        out += f"{sub} Parameters\n\n"
        for n, t in params:
            out += f"{entry} {n}\n\n{render_type_linked(t, here)}\n\n"
    o = f["sig"].get("output")
    if o:
        out += f"{sub} Returns\n\n{render_type_linked(o, here)}\n\n"
    return out


def type_params_section(g: dict, here: str, level: str = "##") -> str:
    ps = []
    for p in g.get("params") or []:
        k = p.get("kind") or {}
        name = p.get("name") or ""
        if "type" in k:
            ps.append((name, render_bounds_linked(k["type"].get("bounds") or [], here)))
        elif "const" in k:
            ps.append((name, ""))
    if not ps:
        return ""
    out = f"{level} Type Parameters\n\n"
    for n, b in ps:
        out += f"{sub_level(level)} {n}\n\n`{n}`" + (f" *extends* {b}" if b else "") + "\n\n"
    return out


def render_item_page(docs: Docs, kind: str, it: dict, here: str) -> str:
    inner = it["inner"][kind]
    body = defined_in(it, here) + "\n" + expand_docs(it, here)

    if kind == "struct":
        sk = inner.get("kind") or {}
        if "plain" in sk:
            fields = sk["plain"].get("fields") or []
            if fields:
                entries = []
                for fid in fields:
                    fl = docs.idx[fid]
                    sf = fl["inner"].get("struct_field")
                    # format 60：struct_field 直接是类型；旧版是 {"type": ...}
                    ft = sf.get("type") if isinstance(sf, dict) and "type" in sf else sf
                    e = f"### {fl['name']}\n\n```rust\n{fl['name']}: {render_type(ft)}\n```\n\n"
                    di = defined_in(fl, here)
                    if di:
                        e += di + "\n"
                    e += expand_docs(fl, here)
                    entries.append(e)
                body += "## Fields\n\n" + "\n***\n\n".join(entries)
            if sk["plain"].get("has_stripped_fields"):
                body += "_（存在非公开字段）_\n\n"
        methods, trait_impls, synthetic = methods_of(docs, it)

    elif kind == "enum":
        variants = inner.get("variants") or []
        if variants:
            entries = []
            for vid in variants:
                v = docs.idx[vid]
                e = f"### {v['name']}\n\n"
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
                    e += "```rust\n(" + ", ".join(tys) + ")\n```\n\n"
                elif "struct" in vk:
                    e += "```rust\n{ .. }\n```\n\n"
                di = defined_in(v, here)
                if di:
                    e += di + "\n"
                e += expand_docs(v, here)
                entries.append(e)
            body += "## Variants\n\n" + "\n***\n\n".join(entries)
        methods, trait_impls, synthetic = methods_of(docs, it)

    elif kind == "trait":
        required, provided = [], []
        for iid in inner.get("items") or []:
            m = docs.idx[iid]
            if "function" not in m["inner"] or m["visibility"] != "public":
                continue
            (provided if m["inner"]["function"].get("has_body") else required).append(m)
        if required:
            body += "## Required Methods\n\n" + "\n***\n\n".join(fn_section(docs, m, here) for m in required)
        if provided:
            body += "## Provided Methods\n\n" + "\n***\n\n".join(fn_section(docs, m, here) for m in provided)
        methods, trait_impls, synthetic = [], [], []

    else:  # function / type_alias / constant / static
        if kind == "function":
            # 上游顺序：标题 → 签名 → Defined in → docs → 各分节
            gen, where = render_generics(inner.get("generics") or EMPTY_G)
            sig = render_fn_sig(it["name"] or "_", inner["sig"], inner.get("header"))
            if gen:
                sig = sig.replace(f"fn {it['name']}(", f"fn {it['name']}{gen}(", 1)
            if where:
                sig += where.rstrip("\n")
            body = "```rust\npub " + sig + "\n```\n\n"
            body += defined_in(it, here) + "\n"
            body += expand_docs(it, here)
            body += type_params_section(inner.get("generics") or EMPTY_G, here)
            params = non_self_inputs(inner["sig"])
            if params:
                body += "## Parameters\n\n"
                for n, t in params:
                    body += f"### {n}\n\n{render_type_linked(t, here)}\n\n"
            o = inner["sig"].get("output")
            if o:
                body += f"## Returns\n\n{render_type_linked(o, here)}\n\n"
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
            body += "## Implementations\n\n" + "\n***\n\n".join(fn_section(docs, m, here) for m in methods)
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
    build_link_targets(docs, found)

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
            # 上游函数页标题带 ()：# Function: createWorkflow()
            title = f"{prefix}: {it['name']}()" if kind == "function" else f"{prefix}: {it['name']}"
            page = render_item_page(docs, kind, it, here=dirname)
            write_page(OUT / dirname / f"{it['name']}.md", it["name"], title, page)
            section += f"- [{it['name']}]({dirname}/{it['name']}.md)\n"
        index_sections.append(section)

    (OUT / "index.md").write_text(
        f"---\nid: {CRATE_SLUG}\ntitle: {CRATE_SLUG}\n---\n\n# {CRATE_SLUG}\n\n"
        + "".join(index_sections),
        encoding="utf-8",
    )

    total = sum(len(v) for v in found.values())
    print(f"生成完毕：{total} 个 item → {OUT}")
    for kind in found:
        print(f"  {kind}: {len(found[kind])}")


if __name__ == "__main__":
    sys.exit(main())
