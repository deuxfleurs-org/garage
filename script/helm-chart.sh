#!/usr/bin/env bash
#
# This is a script used by the CI.
# It packages the helm chart and publish it to a helm repository hosted on S3
# at https://garagehq.deuxfleurs.fr/_helm
#
#   check-version-bump  fails when the chart version was not incremented.
#   publish             package the chart, upload it and refresh the repository index.yaml

set -euo pipefail
shopt -s inherit_errexit

chart_dir=script/helm/garage
helm_repo_s3_uri=s3://garagehq.deuxfleurs.fr/_helm
helm_repo_url=https://garagehq.deuxfleurs.fr/_helm

# extract a field's value from Chart.yaml, reading the file on stdin, trims the value quotes if any
chart_field() {
  sed -n "s/^$1: *//p" | tr -d "\"'"
}

check_version_bump() {
  local pr_target_branch_name=${CI_COMMIT_TARGET_BRANCH:?} pr_target_branch_chart_version pr_chart_version
  git fetch --quiet --depth=1 origin "$pr_target_branch_name"
  pr_target_branch_chart_version=$(git show "FETCH_HEAD:$chart_dir/Chart.yaml" | chart_field version)
  pr_chart_version=$(chart_field version <"$chart_dir/Chart.yaml")
  # TODO: sort will not work with prerelease tags eg 0.11.0-rc.1, relax check if needed:
  if ! printf '%s\n' "$pr_target_branch_chart_version" "$pr_chart_version" | sort --version-sort --unique --check=quiet; then
    echo "Chart version $pr_chart_version must be greater than $pr_target_branch_chart_version on $pr_target_branch_name: bump version in $chart_dir/Chart.yaml" >&2
    exit 1
  fi
  echo "Chart version bumped from $pr_target_branch_chart_version to $pr_chart_version"
}

# template for the s3 command, ommitting only the verb and arguments
aws_s3() {
  AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED \
    aws --endpoint-url https://garage.deuxfleurs.fr --region garage s3 "$@" --only-show-errors
}

publish() {
  local name version workdir tgz
  name=$(chart_field name <"$chart_dir/Chart.yaml")
  version=$(chart_field version <"$chart_dir/Chart.yaml")
  tgz=$name-$version.tgz
  workdir=$(mktemp -d)

  # The index is always rebuilt from the tarballs, by syncing the entire folder locally
  # if this becomes an issue due to volume, we can think of an incremental approach
  aws_s3 sync "$helm_repo_s3_uri" "$workdir"
  if [ -e "$workdir/$tgz" ]; then
    echo "$tgz is already published, not overwriting it"
  else
    helm package "$chart_dir" --destination "$workdir"
    # upload chart before index, so we never references a missing file
    aws_s3 cp "$workdir/$tgz" "$helm_repo_s3_uri/"
    echo "Published $tgz to $helm_repo_url"
  fi
  helm repo index "$workdir" --url "$helm_repo_url"
  aws_s3 cp "$workdir/index.yaml" "$helm_repo_s3_uri/"
  echo "Refreshed $helm_repo_url/index.yaml"
}

case ${1:-} in
check-version-bump) check_version_bump ;;
publish) publish ;;
*)
  echo "usage: $0 check-version-bump|publish" >&2
  exit 2
  ;;
esac
