import ".just/commit.just"

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

# -------------------------------------

# Release a new version, pass --help for options to `uv version --bump`
release bump_level="patch":
    #!/usr/bin/env -S echo-comment --shell-flags="-e" --color bright-green

    ## Exit early if help was requested
    if [[ "{{bump_level}}" == "--help" ]]; then
        uv version --help
        exit 0
    fi

    # 📈 Bump the version in pyproject.toml (patch/minor/major: {{bump_level}})
    uv version --bump {{bump_level}}

    # 📦 Stage all changes (including the version bump)
    git add --all

    # 🔄 Create a temporary commit to capture the new version
    git commit -m "chore(temp): version check"

    # ✂️  Extract the new version number that was just set, undo the commit
    new_version=$(uv version --short)
    git reset --soft HEAD~1

    # ✅ Stage everything again and create the real release commit
    git add --all
    git commit -m  "chore(release): bump 🐍 -> v$new_version"

    # 🏷️ Create the git tag for this release
    git tag -a "py-$new_version" -m "Python Release $new_version"

    branch_name=$(git rev-parse --abbrev-ref HEAD);
    # 🚀 Push the release commit to $branch_name
    git push origin $branch_name

    # 🚀 Push the commit tag to the remote
    git push origin "py-$new_version"

    # ⏳ Wait for CI to build wheels, then download and publish them
    test -z "$(compgen -G 'wheel*/')" || {
      # 🛡️ Safety first: halt if there are leftover wheel* directories from previous runs
      echo "Please delete the wheel*/ dirs:" >&2
      ls wheel*/ -1d >&2
      false
    }

# OLD: no longer use this release approach for Python, CI auto-releases the tag
# Ship a new version as the final step of the release process (idempotent)
ship-wheels mode="":
    # 📥 Download wheel artifacts from the completed CI run
    ## -p wheel* downloads only artifacts matching the "wheel*" pattern
    gh run watch "$(gh run list -L 1 --json databaseId --jq .[0].databaseId)" {{mode}} --exit-status
    gh run download "$(gh run list -L 1 --json databaseId --jq .[0].databaseId)" -p wheel*

    # 🧹 Clean up any existing dist directory and create a fresh one
    rm -rf dist/
    mkdir dist/

    # 🎯 Move all wheel-* artifacts into dist/ and delete their temporary directories
    mv wheel*/* dist/
    rm -rf wheel*/

    # 🎊 Publish the CI-built wheels to PyPI
    uv publish -u __token__ -p $(keyring get PYPIRC_TOKEN "")
