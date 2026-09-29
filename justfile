open-doc:
    cargo doc --workspace --no-deps --open

open-core-doc:
    cargo doc  -p aa-workflow-core  --no-deps --open
generate-reference:
    python3 scripts/generate-docs.py
