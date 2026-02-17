#!/usr/bin/env bash

# https://vaneyckt.io/posts/safer_bash_scripts_with_set_euxo_pipefail/
# Not using "-x" because we aren't debugging.
set -Eeuo pipefail

# We get unbound var err if we don't set arg
arg="${1:-}"

echo "${registry_password}" | helm registry login -u "${registry_user}" --password-stdin "${registry_host}"
helm lint "${chart_dir}"
chart_version=$(grep '^version: ' "${chart_dir}/Chart.yaml" | cut -d' ' -f 2)
helm package "${chart_dir}"

# If chart already exists in the chart repository, don't push.
if helm pull "oci://${registry_host}/${registry_path}/${chart_name}" --version "${chart_version}" &> /dev/null; then
  echo -e "\e[31mWARNING: Chart ${chart_name} version ${chart_version} already exists in the repository.\nThis means that the chart's code has not changed, or you forgot to update the version in Chart.yaml.\e[0m"
else
  if [[ $arg == 'push' ]]; then
    helm push "${chart_name}-${chart_version}.tgz" "oci://${registry_host}/${registry_path}"
  fi
fi
