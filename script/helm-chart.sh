#!/usr/bin/env bash
#
# Package the helm chart as an OCI artifact.
#
#   check-version-bump  fails when the chart version was not incremented.
#   publish             package the chart and push it to $HELM_OCI_REPO

set -euo pipefail
shopt -s inherit_errexit

chart_dir=${CHART_DIR:-script/helm/garage}

chart_field() {
  sed -n "s/^$1: *//p"
}

check_version_bump() {
  local target=${CI_COMMIT_TARGET_BRANCH:?} base head
  git fetch --quiet --depth=1 origin "$target"
  base=$(git show "FETCH_HEAD:$chart_dir/Chart.yaml" | chart_field version)
  head=$(chart_field version <"$chart_dir/Chart.yaml")
  # TODO: sort will not work with prerelease tags eg 0.11.0-rc.1, relax check if needed:
  if ! printf '%s\n' "$base" "$head" | sort --version-sort --unique --check=quiet; then
    echo "Chart version $head must be greater than $base on $target: bump version in $chart_dir/Chart.yaml" >&2
    exit 1
  fi
  echo "Chart version bumped from $base to $head"
}

# Check registry status for a manifest and print status code.
manifest_status() {
  local url=$1 repository=$2 headers status realm service token auth=()
  local accept='Accept: application/vnd.oci.image.manifest.v1+json,application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.v2+json,application/vnd.docker.distribution.manifest.list.v2+json'
  headers=$(mktemp)
  status=$(curl -sS --max-time 30 --head -o /dev/null -D "$headers" -w '%{http_code}' -H "$accept" "$url")
  if [ "$status" != 401 ]; then
    echo "$status"
    return
  fi

  # check for OCI token auth and retry with token:
  realm=$(sed -n 's/^www-authenticate: *bearer .*realm="\([^"]*\)".*/\1/Ip' "$headers")
  service=$(sed -n 's/^www-authenticate: *bearer .*service="\([^"]*\)".*/\1/Ip' "$headers")
  if [ -z "$realm" ]; then
    echo "$url asks for auth, but not with a bearer token" >&2
    exit 1
  fi
  if [ -n "${REGISTRY_USERNAME:-}" ]; then
    auth=(--user "$REGISTRY_USERNAME:${REGISTRY_PASSWORD:?}")
  fi
  token=$(curl -sSf --max-time 30 "${auth[@]}" --get "$realm" \
    --data-urlencode "service=$service" \
    --data-urlencode "scope=repository:$repository:pull" |
    jq -er '.token // .access_token')
  curl -sS --max-time 30 --head -o /dev/null -w '%{http_code}' \
    -H "$accept" -H "Authorization: Bearer $token" "$url"
}

publish() {
  local repo=${HELM_OCI_REPO:?set HELM_OCI_REPO, for example oci://registry-1.docker.io/dxflrs}
  local name version registry repository status package_dir
  name=$(chart_field name <"$chart_dir/Chart.yaml")
  version=$(chart_field version <"$chart_dir/Chart.yaml")
  registry=${repo#oci://}
  registry=${registry%%/*}
  repository=${repo#oci://"$registry"/}/$name

  status=$(manifest_status "https://$registry/v2/$repository/manifests/$version" "$repository")
  case $status in
  200)
    echo "$repo/$name:$version is already published, skipping push"
    return
    ;;
  404) ;;
  *)
    echo "Unexpected HTTP $status from registry for $repo/$name:$version" >&2
    exit 1
    ;;
  esac

  if [ -z "${REGISTRY_USERNAME:-}" ] || [ -z "${REGISTRY_PASSWORD:-}" ]; then
    echo "REGISTRY_USERNAME and REGISTRY_PASSWORD are needed to push $name $version" >&2
    exit 1
  fi
  helm registry login "$registry" --username "$REGISTRY_USERNAME" --password-stdin <<<"$REGISTRY_PASSWORD"
  package_dir=$(mktemp -d)
  helm package "$chart_dir" --destination "$package_dir"
  helm push "$package_dir/$name-$version.tgz" "$repo"
}

case ${1:-} in
check-version-bump) check_version_bump ;;
publish) publish ;;
*)
  echo "usage: $0 check-version-bump|publish" >&2
  exit 2
  ;;
esac
