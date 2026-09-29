open-doc:
    cargo doc --workspace --no-deps --open

open-core-doc:
    cargo doc -p aa_workflow_core --no-deps --open

# 重建 rustdoc JSON（nightly）并生成 docs/reference/——上游 typedoc 同形态
generate-reference:
    python3 scripts/generate-docs.py

open-reference:
    xdg-open docs/reference/index.md
