# Local Flink Setup with Kubernetes Operator (FKO)

This guide provides step-by-step instructions for setting up Apache Flink locally using Minikube and running an end-to-end example with the Flink Kubernetes Operator (FKO).

## 1. Prerequisites
Ensure the following tools are installed:
- **Docker**: For containerized workloads.
- **Minikube**: A local Kubernetes cluster.
- **Helm**: To install the Flink Operator.
- **kubectl**: To interact with Kubernetes.

## 2. Setup Local Kubernetes Cluster
Start a local Kubernetes cluster using Minikube:
```bash
minikube start --cpus 6 --memory 8192
```

## 3. Build Local Flink Image (For Flink 2.0+)
Flink 2.0+ uses a new configuration format (`config.yaml`). To run it with the Flink Kubernetes Operator, you need a custom image that handles the conversion from the operator's default `flink-conf.yaml`.

1. **Build Flink from Source**:
   ```bash
   mvn clean package -DskipTests
   ```
2. **Create a Local Image**:
   Use the `flink-docker` repository to build a local image. Ensure your `Dockerfile` includes `python3` and a custom `docker-entrypoint.sh` that:
   - Copies the read-only `/opt/flink/conf` to a writable `/tmp/flink-conf`.
   - Converts `flink-conf.yaml` into the new `config.yaml` format for Flink 2.0+.
   - Sets `FLINK_CONF_DIR=/tmp/flink-conf`.

3. **Build the Image in Minikube**:
   ```bash
   eval $(minikube -p minikube docker-env)
   docker build -t flink:local .
   ```

## 4. Install Flink Kubernetes Operator
Add the Helm repository and install the operator:
```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.14.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
```

**Crucial for Flink 2.0+**: Patch the operator to use YAML configuration format:
```bash
kubectl patch configmap flink-operator-config --type merge -p '{"data":{"kubernetes.operator.flink.configuration.format":"YAML"}}'
kubectl rollout restart deployment flink-kubernetes-operator
```

## 5. RBAC Setup
```bash
kubectl create serviceaccount flink
kubectl create clusterrolebinding flink-role-binding-flink --clusterrole=edit --serviceaccount=default:flink
```

## 6. Running the Application
Save the following as `flink-application.yaml`:
```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: basic-example
spec:
  image: flink:local
  imagePullPolicy: IfNotPresent
  flinkVersion: v2_0  # Use v2_0 for Flink 2.0+
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    kubernetes.operator.flink.configuration.format: YAML
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
    upgradeMode: stateless
```

Apply the deployment:
```bash
kubectl apply -f flink-application.yaml
```

## 7. Troubleshooting
If your pod crashes with `IllegalConfigurationException`:
- Check if `config.yaml` exists in the container's Flink configuration directory.
- Ensure the JobManager/TaskManager memory is explicitly configured in `flinkConfiguration` or the custom image handles it.
- View logs: `kubectl logs deployment/basic-example`.
