+++
title = "Deploying on Kubernetes"
weight = 32
+++

Garage can also be deployed on a kubernetes cluster via helm chart.

## Deploying

Firstly clone the repository:

```bash
git clone https://git.deuxfleurs.fr/Deuxfleurs/garage
cd garage/scripts/helm
```

Deploy with default options:

```bash
helm install --create-namespace --namespace garage garage ./garage
```

Or deploy with custom values:

```bash
helm install --create-namespace --namespace garage garage ./garage -f values.override.yaml
```

After deploying, cluster layout must be configured manually as described in [Creating a cluster layout](@/documentation/quick-start/_index.md#creating-a-cluster-layout). Use the following command to access garage CLI:

```bash
kubectl exec --stdin --tty -n garage garage-0 -- ./garage status
```

## Overriding default values

All possible configuration values can be found with:

```bash
helm show values ./garage
```

This is an example `values.overrride.yaml` for deploying in a microk8s cluster with a https s3 api ingress route:

```yaml
garage:
  # Make sure to generate a new secret for your deployment
  rpcSecret: "1799bccfd7411eddcf9ebd316bc1f5287ad12a68094e1c6ac6abde7e6feae1ec"

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
