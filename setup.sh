#!/bin/sh
# Run this script in the terminal (./setup.sh) to prepare for deployment using Cloud Code.
echo "Updating helm"
helm repo update
if [ -z "$1" ]
  then
    echo "Changing context to docker-desktop"
    kubectl config use-context docker-desktop
  else
    echo "Using existing Kubernetes context"
fi


echo "Installing spark operator (ignore name in use error)"
helm install my-release spark-operator-1.1.19_fixed.tgz  --create-namespace --namespace spark-operator --set webhook.enable=true || true
echo "Creating spark service account (ignore already exists error)"
kubectl create serviceaccount spark || true
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default || true
