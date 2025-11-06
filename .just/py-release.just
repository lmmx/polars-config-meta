# Full release flow: clean, build, then publish
run-release: clean build publish

# Build the package
build:
    uv build

# Clean previous builds
clean:
    rm -rf dist

# Publish to PyPI
publish:
    uv publish -u __token__ -p $(keyring get PYPIRC_TOKEN "")

# Release a new version, pass --help for options to `uv version --bump`
release bump_level="patch":
    #!/usr/bin/env -S echo-comment --shell-flags="-e" --color bright-green

    # THIS WOULD REQUIRE CI WHEEL BUILDING SCRIPT AND WOULD WORK BEST WITH TRUSTED PUBLISHING

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
ci-ship-wheels mode="":
    #!/usr/bin/env -S echo-comment --shell-flags="-e" --color bright-green
    # THIS WOULD REQUIRE CI WHEEL BUILDING SCRIPT AND WOULD WORK BEST WITH TRUSTED PUBLISHING

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
    just publish
