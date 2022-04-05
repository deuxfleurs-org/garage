Spawn a cluster with minikube

```bash
minikube start
minikube kubectl -- apply -f config.yaml
minikube kubectl -- apply -f daemon.yaml
minikube dashboard

minikube kubectl -- exec -it garage-0 --container garage -- /garage status
# etc.
```


