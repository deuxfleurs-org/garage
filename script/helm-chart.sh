#!/usr/bin/env bash
#
# This is a script used by the CI.
# It packages the helm chart and publish it to a helm repository hosted on S3
# at https://garagehq.deuxfleurs.fr/_helm, and mirrors it as an OCI artifact on the Forgejo registry
# (helm install garage oci://git.deuxfleurs.fr/deuxfleurs/charts/garage)
#
#   check-version-bump  fails when the chart version was not incremented.
#   publish             package the chart, upload it, refresh the repository index.yaml and mirror it to OCI

set -euo pipefail
shopt -s inherit_errexit

chart_dir=script/helm/garage
helm_repo_s3_uri=s3://garagehq.deuxfleurs.fr/_helm
helm_repo_url=https://garagehq.deuxfleurs.fr/_helm
helm_oci_registry=git.deuxfleurs.fr
helm_oci_repo=oci://$helm_oci_registry/deuxfleurs/charts

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

# template for the s3 command, omitting only the verb and arguments
aws_s3() {
  AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED \
    aws --endpoint-url https://garage.deuxfleurs.fr --region garage s3 "$@" --only-show-errors
}

publish() {
  local name version repo new tgz file
  : "${OCI_USERNAME:?}" "${OCI_PASSWORD:?}" # fail if those are not defined so we don't have partial publish
  name=$(chart_field name <"$chart_dir/Chart.yaml")
  version=$(chart_field version <"$chart_dir/Chart.yaml")
  tgz=$name-$version.tgz # name of the target file eg. garage-0.10.4.tgz
  repo=$(mktemp -d) # local copy of the published repository
  new=$(mktemp -d)  # the version to publish
  aws_s3 sync "$helm_repo_s3_uri" "$repo" # we sync the existing repo

  # if the file we built is already listed in the index,
  # abort so we don't erase the release timestamp
  if grep -qF "/$tgz" "$repo/index.yaml" 2>/dev/null; then
    echo "$tgz is already published, not publishing it again"
    file=$repo/$tgz
  else
    # otherwise publish it
    helm package "$chart_dir" --destination "$new"
    # no index to merge on the first publish
    local merge=()
    [ ! -e "$repo/index.yaml" ] || merge=(--merge "$repo/index.yaml")
    helm repo index "$new" --url "$helm_repo_url" "${merge[@]}"
    # upload chart before index, so the index never references a missing file
    aws_s3 cp "$new/$tgz" "$helm_repo_s3_uri/"
    aws_s3 cp "$new/index.yaml" "$helm_repo_s3_uri/"
    echo "Published $tgz to $helm_repo_url"
    file=$new/$tgz
  fi

  # Push the OCI copy to forgejo
  helm registry login "$helm_oci_registry" --username "$OCI_USERNAME" --password-stdin <<<"$OCI_PASSWORD"
  helm push "$file" "$helm_oci_repo"
}

case ${1:-} in
check-version-bump) check_version_bump ;;
publish) publish ;;
*)
  echo "usage: $0 check-version-bump|publish" >&2
  exit 2
  ;;
esac
