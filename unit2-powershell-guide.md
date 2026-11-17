# COSC 3657 Unit 2 PowerShell Demo

Open two PowerShell terminals in the project folder. This walkthrough uses your existing Docker Desktop, `kind-cosc3657-unit2` cluster, and `.venv`.

## 1. Start the controller

In **Terminal 1**, run the controller and leave it running.

```powershell
kubectl config use-context kind-cosc3657-unit2
.\.venv\Scripts\python.exe .\reconciler.py
```

## 2. Apply the assignment manifests

In **Terminal 2**, apply the CRD and sample. Wait until all three Pods show `1/1` and `Running` before moving on. Repeat the last command to check progress.

```powershell
kubectl apply -f .\crd_definition.yaml
# kubectl wait --for=condition=Established crd/resilientdeployments.cosc3657.tu.ca --timeout=60s
kubectl apply -f .\sample_cr.yaml
$selector = 'app.kubernetes.io/managed-by=resilientdeployment-controller'
kubectl -n default get pods -l $selector
```

## 3. Force-delete a Pod and verify recovery

In **Terminal 2**, list the Pod UIDs, force-delete one Pod, then list them again.

```powershell
$columns = 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,STATUS:.status.phase'
kubectl -n default get pods -l $selector -o $columns
$pod = kubectl -n default get pods -l $selector -o 'jsonpath={.items[0].metadata.name}'
kubectl -n default delete pod $pod --force --grace-period=0
kubectl -n default get pods -l $selector -o $columns
```

Repeat the last command until all three Pods are `Running`. The replacement has a new UID even if it reuses the same name. Save the before-and-after output and the controller log showing `desired=3 observed=2 drift=+1`.

## 4. Scale from three to five

In **Terminal 2**, change the desired replica count while the controller is still running.

```powershell
kubectl -n default patch rd echo-service --type=merge --patch 'spec: {replicas: 5}'
kubectl -n default get pods -l $selector
```

Repeat the last command until all five Pods show `1/1` and `Running`
