#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
tag=$(<"$repo_root/VERSION")
chart_version=${tag#v}
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT

if "$repo_root/hack/package-release.sh" v0.0.0 "$output_dir/invalid" 2>/dev/null; then
  echo "package-release.sh accepted a tag that does not match VERSION" >&2
  exit 1
fi

"$repo_root/hack/package-release.sh" "$tag" "$output_dir"

grep -Fq 'REGISTRY="ghcr.io/qiniu"' "$output_dir/install.sh"
grep -Fq "VERSION=\"$tag\"" "$output_dir/install.sh"
tar -xOf "$output_dir/kube-ovn-$chart_version.tgz" kube-ovn/values.yaml |
  grep -Fq 'address: ghcr.io/qiniu'
tar -xOf "$output_dir/kube-ovn-$chart_version.tgz" kube-ovn/values.yaml |
  grep -Fq "DPDK_IMAGE_TAG: $tag-dpdk"
tar -xOf "$output_dir/kube-ovn-v2-$chart_version.tgz" kube-ovn-v2/values.yaml |
  grep -Fq 'repository: ghcr.io/qiniu/vpc-nat-gateway'
tar -xOf "$output_dir/kube-ovn-v2-$chart_version.tgz" kube-ovn-v2/values.yaml |
  grep -Fq "tag: $tag-dpdk"
