export MINIKUBE_PROFILE=stream
first=$(minikube service -n stream list | grep -E 'stream-api' | awk '{print $8}'| awk 'NR==1')
second=$(minikube service -n stream list | grep -E 'stream-http-source' | awk '{print $8}'| awk 'NR==1')
echo "API loadbalancer: $first"
echo "HTTP source loadbalancer: $second"
kubectl get -n stream  configmap/stream-config -o yaml | sed -e  "s#https://stream.keboola.com#$first#g" | sed -e  "s#https://stream-in.keboola.com#$second#g" | kubectl apply -f -
kubectl rollout restart -n stream deployment/stream-api
kubectl rollout restart -n stream deployment/stream-http-source
kubectl rollout restart -n stream deployment/stream-storage-coordinator
kubectl rollout restart -n stream statefulset/stream-storage-writer-reader
