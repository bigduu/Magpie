#!/usr/bin/env bash
# Fill the committed plugin/plugin.json TEMPLATE with a real release's
# version + the real sha256 of the platform archives release.yml already
# built (reusing the `.sha256` sidecars it computes — this script never
# re-hashes anything, only threads the values through).
#
# Magpie ships exactly one artifact per OS key bamboo's plugin schema
# understands (macos / windows / linux — the schema gates by OS only, no
# per-CPU-architecture key):
#   macos   — universal (arm64 + x86_64) apple-darwin tar.gz
#   linux   — x86_64-unknown-linux-gnu tar.gz (runs on x64 hosts; an
#             aarch64 linux build needs a schema follow-up, same per-arch
#             gap nova's plugin README documents)
#   windows — x86_64-pc-windows-msvc zip (also runs under Windows' built-in
#             x64 emulation on ARM64 hosts — the "ship x86_64 for sure"
#             precedent from nova's release pipeline)
#
# Usage:
#   generate-manifest.sh <version> <macos_sha256> <linux_sha256> <windows_sha256> <output_path>
#
#   <version>        e.g. 1.2.3 (no leading "v")
#   <macos_sha256>   sha256 of magpie-v<version>-universal-apple-darwin.tar.gz
#   <linux_sha256>   sha256 of magpie-v<version>-x86_64-unknown-linux-gnu.tar.gz
#   <windows_sha256> sha256 of magpie-v<version>-x86_64-pc-windows-msvc.zip
#   <output_path>    where to write the generated plugin.json
#
# Dry-run locally, e.g.:
#   ./generate-manifest.sh 9.9.9 "$(printf 'a%.0s' {1..64})" "$(printf 'b%.0s' {1..64})" "$(printf 'c%.0s' {1..64})" /tmp/plugin.json

set -euo pipefail

if [ "$#" -ne 5 ]; then
  echo "usage: $0 <version> <macos_sha256> <linux_sha256> <windows_sha256> <output_path>" >&2
  exit 1
fi

VERSION="$1"
MACOS_SHA256="$2"
LINUX_SHA256="$3"
WINDOWS_SHA256="$4"
OUTPUT_PATH="$5"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$SCRIPT_DIR/plugin.json"

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+].*)?$ ]]; then
  echo "error: version '$VERSION' is not a plausible semver (N.N.N[-pre][+build])" >&2
  exit 1
fi

for sha in "$MACOS_SHA256" "$LINUX_SHA256" "$WINDOWS_SHA256"; do
  if ! [[ "$sha" =~ ^[0-9a-f]{64}$ ]]; then
    echo "error: sha256 '$sha' is not 64 lowercase hex chars" >&2
    exit 1
  fi
done

MACOS_ASSET="magpie-v${VERSION}-universal-apple-darwin.tar.gz"
LINUX_ASSET="magpie-v${VERSION}-x86_64-unknown-linux-gnu.tar.gz"
WINDOWS_ASSET="magpie-v${VERSION}-x86_64-pc-windows-msvc.zip"
BASE_URL="https://github.com/bigduu/Magpie/releases/download/v${VERSION}"

mkdir -p "$(dirname "$OUTPUT_PATH")"

jq \
  --arg version "$VERSION" \
  --arg macos_url "${BASE_URL}/${MACOS_ASSET}" \
  --arg macos_sha "$MACOS_SHA256" \
  --arg linux_url "${BASE_URL}/${LINUX_ASSET}" \
  --arg linux_sha "$LINUX_SHA256" \
  --arg windows_url "${BASE_URL}/${WINDOWS_ASSET}" \
  --arg windows_sha "$WINDOWS_SHA256" \
  '.version = $version
   | .artifacts.macos.url = $macos_url
   | .artifacts.macos.sha256 = $macos_sha
   | .artifacts.linux.url = $linux_url
   | .artifacts.linux.sha256 = $linux_sha
   | .artifacts.windows.url = $windows_url
   | .artifacts.windows.sha256 = $windows_sha' \
  "$TEMPLATE" > "$OUTPUT_PATH"

echo "Generated $OUTPUT_PATH (version=$VERSION, macos=$MACOS_SHA256, linux=$LINUX_SHA256, windows=$WINDOWS_SHA256)"
