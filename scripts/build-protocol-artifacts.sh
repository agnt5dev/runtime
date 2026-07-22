#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="${1:-}"
output_dir="${repo_root}/dist/protocol"

if [[ -z "${version}" ]]; then
  version="$(
    cd "${repo_root}"
    cargo metadata --no-deps --format-version 1 \
      | jq -r '.packages[] | select(.name == "agnt5-proto") | .version'
  )"
fi

if [[ -z "${version}" || "${version}" == "null" ]]; then
  echo "Unable to determine agnt5-proto version." >&2
  exit 1
fi

for command in buf git jq; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "${command} is required to build protocol artifacts." >&2
    exit 1
  fi
done

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

mkdir -p "${output_dir}"
find "${output_dir}" -mindepth 1 -maxdepth 1 -type f -delete

descriptor="agnt5-protocol-v${version}.binpb"
(
  cd "${repo_root}/proto"
  buf build \
    --path agnt5/protocol/v2 \
    --as-file-descriptor-set \
    --exclude-source-info \
    -o "${output_dir}/${descriptor}"
)

spec_dir="${repo_root}/proto/agnt5/protocol/v2/spec"
cp "${repo_root}/proto/agnt5/protocol/v2/README.md" "${output_dir}/README.md"
for file in \
  capabilities.json \
  compatibility.json \
  declarations.md \
  error-mapping.json \
  lifecycle.md \
  protocol-lock.schema.json \
  sdk-synchronization.md \
  transports.md; do
  cp "${spec_dir}/${file}" "${output_dir}/${file}"
done

fixture_dir="${repo_root}/tests/conformance/v2/fixtures"
for file in "${fixture_dir}"/*.json; do
  cp "${file}" "${output_dir}/$(basename "${file}")"
done

descriptor_digest="$(sha256_file "${output_dir}/${descriptor}")"
files_json="$({
  for file in "${output_dir}"/*; do
    name="$(basename "${file}")"
    if [[ "${name}" == "${descriptor}" ]]; then
      continue
    fi
    jq -n \
      --arg name "${name}" \
      --arg sha256 "$(sha256_file "${file}")" \
      '{name: $name, sha256: $sha256}'
  done
} | jq -s 'sort_by(.name)')"

source_commit="$(git -C "${repo_root}" rev-parse HEAD)"
wire_minor="$(jq -r '.wire_version.minor' "${spec_dir}/compatibility.json")"
lock_file="agnt5-protocol-v${version}.lock.json"
jq -n \
  --arg release_tag "protocol/v${version}" \
  --arg artifact_version "${version}" \
  --arg source_commit "${source_commit}" \
  --arg descriptor_name "${descriptor}" \
  --arg descriptor_sha256 "${descriptor_digest}" \
  --argjson wire_minor "${wire_minor}" \
  --argjson files "${files_json}" \
  '{
    schema_version: 1,
    protocol_package: "agnt5.protocol.v2",
    wire_version: {major: 2, minor: $wire_minor},
    release_tag: $release_tag,
    artifact_version: $artifact_version,
    source_commit: $source_commit,
    descriptor: {name: $descriptor_name, sha256: $descriptor_sha256},
    projections: {
      rust: {package: "agnt5-proto", version: $artifact_version},
      go: {module: "github.com/agnt5dev/runtime/gen/go", version: ("v" + $artifact_version)}
    },
    files: $files
  }' > "${output_dir}/${lock_file}"

jq -e '
  .schema_version == 1 and
  .protocol_package == "agnt5.protocol.v2" and
  .wire_version.major == 2 and
  (.descriptor.sha256 | test("^[0-9a-f]{64}$")) and
  (.files | length > 0)
' "${output_dir}/${lock_file}" >/dev/null

(
  cd "${output_dir}"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum ./* > SHA256SUMS
    sha256sum -c SHA256SUMS
  else
    shasum -a 256 ./* > SHA256SUMS
    shasum -a 256 -c SHA256SUMS
  fi
)

echo "Built AGNT5 protocol ${version} artifacts in ${output_dir}"
