# Running a spark application using Spark Operator
The simplest steps to run a spark application using Spark Operator are:
* [Install Helm](https://helm.sh/docs/intro/install/)
* [Install the spark operator using Helm](#Install-the-spark-operator-using-Helm) (if you haven't already)
* Press the Play button on the Run configuration in PyCharm

# TL;DR
1. Install Cloud Code ([IDE plugin](https://cloud.google.com/code/docs))
2. [Install WSL2 on Windows (recommended for Windows)](#Install-WSL2-on-Windows)
3. [Docker for Desktop must be installed](https://docs.docker.com/desktop/)
4. [Install Helm](https://helm.sh/docs/intro/install/)
5. Run the setup.sh script in the terminal
    ```shell
    ./setup.sh)
    ```

**If you do the above, then you don't need to read the following.**
Here's a bit more information.

# Install WSL2 on Windows
*Only needed for Windows operating system*.

On windows, I generally use WSL2 rather than the Windows command prompt, 
because nearly all Kubernetes and Docker examples and help are written for Linux scripts. 
Here are guides for the WSL2 install:
* [WSL for Windows 11](https://pureinfotech.com/install-wsl-windows-11/)
* [WSL for Windows 10 install](https://ubuntu.com/tutorials/install-ubuntu-on-wsl2-on-windows-10#1-overview)
* [Nick's old but good guide to WSL2](https://nickjanetakis.com/blog/a-linux-dev-environment-on-windows-with-wsl-2-docker-desktop-and-more)

Once installed, run all the scripts in the WSL2 (Ubuntu  terminal).
Note that your there will be Kubernetes settings in ~/.kube/config

# Install the spark operator using Helm
Install Helm. See [Helm install](https://helm.sh/docs/intro/install/).
(On WSL2 or Linux, follow the Debian/Ubuntu section)

**Always** update your local cache:
```shell
helm repo update
```
You can list your charts or those available with:
```shell
helm list -A
helm search repo
```

Ensure that you are using the correct context (local desktop or remote cluster)
```shell
kubectl config get-contexts
kubectl config use-context  docker-desktop
```

Install the operator and it's webhook. (If you set the sparkJobNamespace in the operator install, 
then the [namespace](https://kubernetes.io/docs/tasks/administer-cluster/namespaces-walkthrough/) must exist and it 
must match that specified in the SparkApplication yaml file.)
From the [Spark Operatior documentation](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/quick-start-guide.md)
You could do the following:
```shell
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install my-release spark-operator/spark-operator  --create-namespace --namespace spark-operator --set webhook.enable=true
```
But **I recommend the following instead**. That's because if you want to use jupyter notebook with spark-operator
(or any other software that required exposed ports),
then you'll need a fixed version of the operator newer than 1.1.19. (See [issue 1401](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/issues/1404).)
You can install my fixed version (included herein), as follows:
```shell
helm install my-release spark-operator-1.1.19_fixed.tgz  --create-namespace --namespace spark-operator --set webhook.enable=true
```

If necessary, you can uninstall a helm chart by referring to its name:
```shell
 helm uninstall my-release --namespace spark-operator
```
The name, my-release, is arbitrary; you can give it any name.


# Create Spark account
Ensure a spark service account, according to [RBAC guide](https://spark.apache.org/docs/latest/running-on-kubernetes.html).
```shell
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
kubectl get serviceaccounts
```

# Cloud Code plugin run play button
Once the above is done, manually or by the setup.sh script, then you can press the Cloud Code ([IDE plugin](https://cloud.google.com/code/docs)) 
Run configuration play button. The first run will take a longer time because the Docker image must be created. 
It may take several minutes on a slow internet connection.

