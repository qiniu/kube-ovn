#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT

ruby -ryaml - "$repo_root/.github/workflows/publish-images.yaml" <<'RUBY'
workflow = YAML.load_file(ARGV.fetch(0))
steps = workflow.fetch("jobs").fetch("build-release-images").fetch("steps")
setup_buildx = steps.find { |step| step["uses"] == "docker/setup-buildx-action@v3" }
unless setup_buildx&.dig("with", "driver") == "docker"
  raise "build-release-images must use the docker driver to consume daemon-local base images"
end
RUBY

if "$repo_root/hack/package-release.sh" release-1.15.10-alpha.1 "$output_dir/invalid" 2>/dev/null; then
  echo "package-release.sh accepted an invalid release tag" >&2
  exit 1
fi

for tag in v1.15.10 v1.15.10-alpha.1 v1.15.10-rc.1 v1.15.10-qiniu.1 v1.15.11-alpha.1; do
  tag_output_dir="$output_dir/${tag#v}"
  "$repo_root/hack/package-release.sh" "$tag" "$tag_output_dir"

  grep -Fq 'REGISTRY="ghcr.io/qiniu"' "$tag_output_dir/install.sh"
  grep -Fq "VERSION=\"$tag\"" "$tag_output_dir/install.sh"
  tar -xOf "$tag_output_dir/kube-ovn-${tag#v}.tgz" kube-ovn/Chart.yaml |
    grep -Fq "version: ${tag#v}"
  tar -xOf "$tag_output_dir/kube-ovn-${tag#v}.tgz" kube-ovn/Chart.yaml |
    grep -Fq "appVersion: ${tag#v}"
  tar -xOf "$tag_output_dir/kube-ovn-${tag#v}.tgz" kube-ovn/values.yaml |
    grep -Fq 'address: ghcr.io/qiniu'
  tar -xOf "$tag_output_dir/kube-ovn-${tag#v}.tgz" kube-ovn/values.yaml |
    grep -Fq "DPDK_IMAGE_TAG: $tag-dpdk"
  tar -xOf "$tag_output_dir/kube-ovn-v2-${tag#v}.tgz" kube-ovn-v2/Chart.yaml |
    grep -Fq "version: ${tag#v}"
  tar -xOf "$tag_output_dir/kube-ovn-v2-${tag#v}.tgz" kube-ovn-v2/Chart.yaml |
    grep -Fq "appVersion: ${tag#v}"
  tar -xOf "$tag_output_dir/kube-ovn-v2-${tag#v}.tgz" kube-ovn-v2/values.yaml |
    grep -Fq 'repository: ghcr.io/qiniu/vpc-nat-gateway'
  tar -xOf "$tag_output_dir/kube-ovn-v2-${tag#v}.tgz" kube-ovn-v2/values.yaml |
    grep -Fq "tag: $tag-dpdk"
done
