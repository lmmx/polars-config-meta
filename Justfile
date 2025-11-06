import ".just/commit.just"
import ".just/py-release.just"

default: ruff-check

ci_opt := if env("PRE_COMMIT_HOME", "") != "" { "-ci" } else { "" }

precommit:
    just pc{{ci_opt}}

pc:     fmt code-quality lint
pc-fix: fmt code-quality-fix
pc-ci:      code-quality

prepush: py

# (Not running ty in lint recipe)
lint: ruff-check # lint-action

fmt:     ruff-fmt code-quality-fix

full:    pc prepush test py
full-ci: pc-ci prepush         py

# usage:
#   just e                -> open Justfile normally
#   just e foo            -> search for "foo" and open Justfile at that line
#   just e @bar           -> search for "^bar" (recipe name) and open Justfile at that line
#
e target="":
    #!/usr/bin/env -S echo-comment --color bold-red
    if [[ "{{target}}" == "" ]]; then
      $EDITOR Justfile
    else
      pat="{{target}}"
      if [[ "$pat" == @* ]]; then
        pat="^${pat:1}"   # strip @ and prefix with ^
      fi
      line=$(rg -n "$pat" Justfile | head -n1 | cut -d: -f1)
      if [[ -n "$line" ]]; then
        $EDITOR +$line Justfile
      else
        # No match for: $pat
        exit 1
      fi
    fi

lint-action:
    actionlint .github/workflows/CI.yml

# -------------------------------------

test *args:
    just py-test {{args}}

# -------------------------------------

ruff-check mode="":
   ruff check . {{mode}}

ruff-fix:
   just ruff-check --fix

ruff-fmt:
   ruff format .

# Type checking
ty *args:
   #!/usr/bin/env bash
   ty check . --exit-zero {{args}} 2> >(grep -v "WARN ty is pre-release software" >&2)

t:
   just ty --output-format=concise

tv:
   just t | rg -v 'has no attribute \`permute'

pf:
    pyrefly check . --output-format=min-text

# -------------------------------------

py: py-test

# Test Python plugin with pytest
py-test *args:
    #!/usr/bin/env bash
    $(uv python find) -m pytest tests/ {{args}}

py-schema:
    $(uv python find) schema_demo.py

# -------------------------------------

install-hooks:
   pre-commit install

run-pc:
   pre-commit run --all-files

setup:
   #!/usr/bin/env bash
   uv venv
   source .venv/bin/activate
   uv sync

sync:
   uv sync

# -------------------------------------

fix-eof-ws mode="":
    #!/usr/bin/env sh
    ARGS=''
    if [ "{{mode}}" = "check" ]; then
        ARGS="--check-only"
    fi
    whitespace-format --add-new-line-marker-at-end-of-file \
          --new-line-marker=linux \
          --normalize-new-line-markers \
          --exclude ".git/|target/|dist/|\.swp|.egg-info/|\.so$|.json$|.lock$|.parquet$|.venv/|.stubs/|\..*cache/" \
          $ARGS \
          .

code-quality:
    # just ty-ci
    taplo lint
    taplo format --check
    just fix-eof-ws check

code-quality-fix:
    taplo lint
    taplo format
    just fix-eof-ws

# -------------------------------------

mkdocs command="build":
    $(uv python find) -m mkdocs {{command}}
