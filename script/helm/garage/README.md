# garage

![Version: 0.10.1](https://img.shields.io/badge/Version-0.10.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v2.4.0](https://img.shields.io/badge/AppVersion-v2.4.0-informational?style=flat-square)

S3-compatible object store for small self-hosted geo-distributed deployments

**Homepage:** <https://garagehq.deuxfleurs.fr/>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Garage maintainer team | <garagehq@deuxfleurs.fr> |  |

## Source Code

* <https://git.deuxfleurs.fr/Deuxfleurs/garage.git>

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| args | list | `[]` | Override the container arguments. |
| command | list | `[]` | Override the container entrypoint. |
| commonLabels | object | `{}` | Additional labels to add to all resources created by this chart |
| deployment.kind | string | `"StatefulSet"` | Switchable to DaemonSet |
| deployment.podManagementPolicy | string | `"OrderedReady"` | If using statefulset, allow Parallel or OrderedReady (default) |
| deployment.replicaCount | int | `3` | Number of StatefulSet replicas/garage nodes to start |
| environment | object | `{}` | Extra container env vars, as a list of {name, value} objects (same shape as a Pod container's env) |
| extraVolumeMounts | object | `{}` | Extra volume mounts, as a list of mount objects (same shape as a container's volumeMounts) |
| extraVolumes | object | `{}` | Extra volumes, as a list of volume objects (same shape as a PodSpec's volumes) |
| fullnameOverride | string | `""` |  |
| garage.additionalTopLevelConfig | string | `""` | Additional configuration to append to garage.toml. Use a multi-line string for custom config. Example:  additionalTopLevelConfig: |-    data_fsync = true |
| garage.admin.apiBindAddr | string | `"[::]:3903"` |  |
| garage.blockSize | string | `"1048576"` | Defaults is 1MB, an increase can result in better performance in certain scenarios https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#block_size |
| garage.bootstrapPeers | list | `[]` | This is not required if you use the integrated kubernetes discovery |
| garage.compressionLevel | string | `"1"` | zstd compression level of stored blocks https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#compression_level |
| garage.consistencyMode | string | `"consistent"` | By default, enable read-after-write consistency guarantees, see the consistency_mode section at https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#consistency_mode |
| garage.dbEngine | string | `"lmdb"` | Can be changed for better performance on certain systems, use "sqlite" to prioritize durability https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#db_engine |
| garage.existingConfigMap | string | `""` | if not empty string, allow using an existing ConfigMap for the garage.toml, if set, ignores garage.toml |
| garage.existingRpcSecret | string | `""` | If you want to provide an rpcSecret within an existing k8s secret, specify the secret name here, and store the value under the secret key `rpcSecret` the default secret will not be created |
| garage.garageTomlString | string | `""` | String Template for the garage configuration if set, ignores above values. Values can be templated, see https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/ |
| garage.kubernetesSkipCrd | bool | `false` | Set to true if you want to use k8s discovery but install the CRDs manually outside of the helm chart, for example if you operate at namespace level without cluster resources |
| garage.metadataAutoSnapshotInterval | string | `""` | If this value is set, Garage will automatically take a snapshot of the metadata DB file at a regular interval and save it in the metadata directory. https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#metadata_auto_snapshot_interval |
| garage.noClusterRole | bool | `false` | Set to true if you want to use roles instead of cluster roles |
| garage.replicationFactor | string | `"3"` | Default to 3 replicas, see the replication_factor section at https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#replication_factor |
| garage.rpcBindAddr | string | `"[::]:3901"` | Port used for node-to-node RPC |
| garage.rpcSecret | string | `""` | If not given, a random secret will be generated and stored in a Secret object |
| garage.s3.api.bindAddr | string | `"[::]:3900"` |  |
| garage.s3.api.region | string | `"garage"` |  |
| garage.s3.api.rootDomain | string | `".s3.garage.tld"` |  |
| garage.s3.web.bindAddr | string | `"[::]:3902"` |  |
| garage.s3.web.index | string | `"index.html"` |  |
| garage.s3.web.rootDomain | string | `".web.garage.tld"` |  |
| garage.singleNode | bool | `false` | Start Garage with `--single-node`, run one StatefulSet replica, and render replication_factor = 1 in the generated garage.toml, if using garageTomlString or existingConfigMap, set replication_factor = 1 yourself. |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"dxflrs/amd64_garage"` | default to amd64 docker image |
| image.tag | string | `""` | set the image tag, please prefer using the chart version and not this, to avoid compatibility issues |
| imagePullSecrets | list | `[]` | set if you need credentials to pull your custom image |
| ingress.s3.api.annotations | object | `{}` | Rely _either_ on the className or the annotation below but not both! If you want to use the className, set className: "nginx" and replace "nginx" by an Ingress controller name, examples [here](https://kubernetes.io/docs/concepts/services-networking/ingress-controllers). |
| ingress.s3.api.enabled | bool | `false` |  |
| ingress.s3.api.hosts[0] | object | `{"host":"s3.garage.tld","paths":[{"path":"/","pathType":"Prefix"}]}` | garage S3 API endpoint, to be used with awscli for example |
| ingress.s3.api.hosts[1] | object | `{"host":"*.s3.garage.tld","paths":[{"path":"/","pathType":"Prefix"}]}` | garage S3 API endpoint, DNS style bucket access |
| ingress.s3.api.labels | object | `{}` |  |
| ingress.s3.api.tls | list | `[]` |  |
| ingress.s3.web.annotations | object | `{}` | Rely _either_ on the className or the annotation below but not both! If you want to use the className, set className: "nginx" and replace "nginx" by an Ingress controller name, examples [here](https://kubernetes.io/docs/concepts/services-networking/ingress-controllers). |
| ingress.s3.web.enabled | bool | `false` |  |
| ingress.s3.web.hosts[0] | object | `{"host":"*.web.garage.tld","paths":[{"path":"/","pathType":"Prefix"}]}` | wildcard website access with bucket name prefix |
| ingress.s3.web.hosts[1] | object | `{"host":"mywebpage.example.com","paths":[{"path":"/","pathType":"Prefix"}]}` | specific bucket access with FQDN bucket |
| ingress.s3.web.labels | object | `{}` |  |
| ingress.s3.web.tls | list | `[]` |  |
| initImage.pullPolicy | string | `"IfNotPresent"` |  |
| initImage.repository | string | `"busybox"` |  |
| initImage.tag | string | `"stable"` |  |
| livenessProbe | object | `{}` | Specifies a livenessProbe |
| monitoring.metrics.enabled | bool | `false` | If true, a service for monitoring is created with a prometheus.io/scrape annotation |
| monitoring.metrics.serviceMonitor.enabled | bool | `false` | If true, a ServiceMonitor CRD is created for a prometheus operator https://github.com/coreos/prometheus-operator |
| monitoring.metrics.serviceMonitor.interval | string | `"15s"` |  |
| monitoring.metrics.serviceMonitor.labels | object | `{}` |  |
| monitoring.metrics.serviceMonitor.path | string | `"/metrics"` |  |
| monitoring.metrics.serviceMonitor.relabelings | list | `[]` |  |
| monitoring.metrics.serviceMonitor.scheme | string | `"http"` |  |
| monitoring.metrics.serviceMonitor.scrapeTimeout | string | `"10s"` |  |
| monitoring.metrics.serviceMonitor.tlsConfig | object | `{}` |  |
| monitoring.tracing.sink | string | `""` | specify a sink endpoint for OpenTelemetry Traces, eg. `http://localhost:4317` |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| persistence.data.hostPath | string | `"/var/lib/garage/data"` |  |
| persistence.data.size | string | `"100Mi"` |  |
| persistence.enabled | bool | `true` |  |
| persistence.meta.hostPath | string | `"/var/lib/garage/meta"` |  |
| persistence.meta.size | string | `"100Mi"` |  |
| podAnnotations | object | `{}` | additional pod annotations |
| podSecurityContext.fsGroup | int | `1000` |  |
| podSecurityContext.fsGroupChangePolicy | string | `"OnRootMismatch"` |  |
| podSecurityContext.runAsGroup | int | `1000` |  |
| podSecurityContext.runAsNonRoot | bool | `true` |  |
| podSecurityContext.runAsUser | int | `1000` |  |
| priorityClassName | string | `""` | Optional priority class name to assign to the pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/ |
| readinessProbe | object | `{}` | Specifies a readinessProbe |
| resources | object | `{}` |  |
| securityContext.capabilities | object | `{"drop":["ALL"]}` | The default security context is heavily restricted, feel free to tune it to your requirements |
| securityContext.readOnlyRootFilesystem | bool | `true` |  |
| service.annotations | object | `{}` | Annotations to add to the service |
| service.s3.api.port | int | `3900` |  |
| service.s3.web.port | int | `3902` |  |
| service.type | string | `"ClusterIP"` | You can rely on any service to expose your cluster - ClusterIP (+ Ingress) - NodePort (+ Ingress) - LoadBalancer |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template |
| tolerations | list | `[]` |  |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
