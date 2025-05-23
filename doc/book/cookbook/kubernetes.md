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

If you want to manage the CustomRessourceDefinition used by garage for its `kubernetes_discovery` outside of the helm chart, add `garage.kubernetesSkipCrd: true` to your custom values and use the kustomization before deploying the helm chart:

```bash
kubectl apply -k ../k8s/crd
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
  # Use only 2 replicas per object
  replicationFactor: 2

# Start 4 instances (StatefulSets) of garage
deployment:
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

## Increase PVC size on running Garage instances

Since the Garage Helm chart creates the data and meta PVC based on `StatefulSet` templates, increasing the PVC size can be a bit tricky.

### Confirm the `StorageClass` used for Garage supports volume expansion

Confirm the storage class used for garage.

```bash
kubectl -n garage get pvc 
NAME            STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS     VOLUMEATTRIBUTESCLASS   AGE
data-garage-0   Bound    pvc-080360c9-8ce3-4acf-8579-1701e57b7f3f   30Gi       RWO            longhorn-local   <unset>                 77d
data-garage-1   Bound    pvc-ab8ba697-6030-4fc7-ab3c-0d6df9e3dbc0   30Gi       RWO            longhorn-local   <unset>                 5d8h
data-garage-2   Bound    pvc-3ab37551-0231-4604-986d-136d0fd950ec   30Gi       RWO            longhorn-local   <unset>                 5d5h
meta-garage-0   Bound    pvc-3b457302-3023-4169-846e-c928c5f2ea65   3Gi        RWO            longhorn-local   <unset>                 77d
meta-garage-1   Bound    pvc-49ace2b9-5c85-42df-9247-51c4cf64b460   3Gi        RWO            longhorn-local   <unset>                 5d8h
meta-garage-2   Bound    pvc-99e2e50f-42b4-4128-ae2f-b52629259723   3Gi        RWO            longhorn-local   <unset>                 5d5h
```

In this case, the storage class is `longhorn-local`. Now, check if `ALLOWVOLUMEEXPANSION` is true for the used `StorageClass`.

```bash
kubectl get storageclasses.storage.k8s.io longhorn-local
NAME             PROVISIONER          RECLAIMPOLICY   VOLUMEBINDINGMODE   ALLOWVOLUMEEXPANSION   AGE
longhorn-local   driver.longhorn.io   Delete          Immediate           true                   103d
```

If your `StorageClass` does not support volume expansion, double check if you can enable it. Otherwise, your only real option is to spin up a new Garage cluster with increased size and migrate all data over.

If your `StorageClass` supports expansion, you are free to continue.

### Increase the size of the PVCs

Increase the size of all PVCs to your desired size.

```bash
kubectl -n garage edit pvc data-garage-0
kubectl -n garage edit pvc data-garage-1
kubectl -n garage edit pvc data-garage-2
kubectl -n garage edit pvc meta-garage-0
kubectl -n garage edit pvc meta-garage-1
kubectl -n garage edit pvc meta-garage-2
```

### Increase the size of the `StatefulSet` PVC template

This is an optional step, but if not done, future instances of Garage will be created with the original size from the template.

```bash
kubectl -n garage delete sts --cascade=orphan garage 
statefulset.apps "garage" deleted
```

This will remove the Garage `StatefulSet` but leave the pods running. It may seem destructive but needs to be done this way since edits to the size of PVC templates are prohibited.

### Redeploy the `StatefulSet`

Now the size of future PVCs can be increased, and the Garage Helm chart can be upgraded. The new `StatefulSet` should take ownership of the orphaned pods again.
