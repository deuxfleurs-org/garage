# Garage helm3 chart

This chart deploys garage on a kubernetes cluster.

## Deploying

With default options:

```bash
helm install --create-namespace --namespace garage garage ./garage
```

With custom values:

```bash
helm install --create-namespace --namespace garage garage ./garage -f values.override.yaml
```

## Overriding default values

All possible configuration values can be found in [values.yaml](garage/values.yaml).

This is an example `values.overrride.yaml` for deploying in a microk8s cluster with a https s3 api ingress route:

```yaml
# Start 4 instances (StatefulSets) of garage
replicaCount: 4

# Override default storage class and size
persistence:
  meta:
    storageClass: "openebs-hostpath"
    size: 100Mi
  data:
    storageClass: "openebs-hostpath"
    size: 1Gi

ingress:
  s3:
    api:
      enabled: true
      className: "public"
      annotations:
        cert-manager.io/cluster-issuer: "letsencrypt-prod"
        nginx.ingress.kubernetes.io/proxy-body-size: 500m
      hosts:
        - host: s3-api.my-domain.com
          paths:
            - path: /
              pathType: Prefix
      tls:
        - secretName: garage-ingress-cert
          hosts:
            - s3-api.my-domain.com
```

## Removing

```bash
helm delete --namespace garage garage
```

Note that this will leave behind custom CRD `garagenodes.deuxfleurs.fr`, which must be removed manually if desired.
