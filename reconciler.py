import logging
from itertools import count
from threading import Event, Thread

from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException
from kubernetes.config import ConfigException
from urllib3.exceptions import HTTPError


GROUP = "cosc3657.tu.ca"
VERSION = "v1alpha1"
KIND = "ResilientDeployment"
RESOURCE = {"group": GROUP, "version": VERSION, "plural": "resilientdeployments"}
MANAGED_LABEL = "app.kubernetes.io/managed-by"
MANAGER = "resilientdeployment-controller"
POD_SELECTOR = f"{MANAGED_LABEL}={MANAGER}"
REQUEST_TIMEOUT = (3, 10)
RESYNC_SECONDS = 5
API_ERRORS = (ApiException, HTTPError, OSError)
LOG = logging.getLogger(__name__)


def delete_pod(core, namespace, pod):
    # A retry must never delete a different Pod that has reused this name.
    options = client.V1DeleteOptions(
        preconditions=client.V1Preconditions(uid=pod.metadata.uid)
    )
    try:
        core.delete_namespaced_pod(
            pod.metadata.name, namespace, body=options,
            _request_timeout=REQUEST_TIMEOUT,
        )
    except ApiException as error:
        if error.status not in (404, 409):
            raise


def reconcile(core, resource):
    metadata = resource["metadata"]
    if metadata.get("deletionTimestamp"):
        return

    namespace = metadata["namespace"]
    uid = metadata["uid"]
    spec = resource.get("spec")
    if not spec:
        LOG.warning("Skipping %s/%s because spec is missing", namespace, metadata["name"])
        return
    pods = core.list_namespaced_pod(
        namespace, label_selector=POD_SELECTOR, _request_timeout=REQUEST_TIMEOUT
    ).items
    owned = [
        pod for pod in pods
        if any(
            owner.controller and owner.uid == uid
            for owner in pod.metadata.owner_references or []
        )
    ]
    active = []
    for pod in owned:
        if pod.metadata.deletion_timestamp:
            continue
        if pod.status and pod.status.phase in ("Failed", "Succeeded"):
            delete_pod(core, namespace, pod)
        else:
            # Pending Pods count as replicas while Kubernetes schedules them.
            active.append(pod)

    desired = spec["replicas"]
    drift = desired - len(active)
    if drift == 0:
        return
    LOG.info(
        "%s/%s desired=%d observed=%d drift=%+d",
        namespace, metadata["name"], desired, len(active), drift,
    )

    if drift < 0:
        ordered = sorted(active, key=lambda pod: pod.metadata.name, reverse=True)
        excess = -drift
        for pod in ordered[:excess]:
            delete_pod(core, namespace, pod)
        return

    # Stable names make creation safe to retry after an ambiguous API response.
    occupied = {pod.metadata.name for pod in pods}
    candidates = (f"rd-{uid}-{index}" for index in count())
    available = (name for name in candidates if name not in occupied)
    owner = client.V1OwnerReference(
        api_version=f"{GROUP}/{VERSION}", kind=KIND,
        name=metadata["name"], uid=uid, controller=True,
    )
    for _ in range(drift):
        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=next(available), labels={MANAGED_LABEL: MANAGER},
                owner_references=[owner],
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[
                    client.V1Container(
                        name="workload", image=spec["image"], args=spec.get("args")
                    )
                ],
            ),
        )
        try:
            core.create_namespaced_pod(namespace, pod, _request_timeout=REQUEST_TIMEOUT)
        except ApiException as error:
            if error.status != 409:
                raise
            # State changed since the list. Recount before creating more Pods.
            return


def watch_changes(list_method, changed, stopping, **kwargs):
    while not stopping.is_set():
        stream = watch.Watch()
        try:
            for event in stream.stream(
                list_method, timeout_seconds=30, _request_timeout=(3, 40), **kwargs
            ):
                if stopping.is_set():
                    break
                if event and event["type"] in ("ADDED", "MODIFIED", "DELETED"):
                    changed.set()
        except API_ERRORS as error:
            LOG.warning("Watch disconnected (%s): %s", list_method.__name__, error)
        finally:
            stream.stop()

        # Each reconnect prompts a fresh read, including after an expired watch.
        changed.set()
        stopping.wait(1)


def reconcile_all(custom, core):
    try:
        resources = custom.list_cluster_custom_object(
            **RESOURCE, _request_timeout=REQUEST_TIMEOUT
        )["items"]
    except API_ERRORS as error:
        LOG.warning("Could not list %s resources: %s", KIND, error)
        return

    for resource in resources:
        try:
            reconcile(core, resource)
        except API_ERRORS as error:
            metadata = resource["metadata"]
            LOG.warning("Could not reconcile %s/%s: %s",
                        metadata["namespace"], metadata["name"], error)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        config.load_kube_config()
    except ConfigException as error:
        LOG.error("Could not load kubeconfig: %s", error)
        return 1

    custom = client.CustomObjectsApi()
    core = client.CoreV1Api()
    changed = Event()
    stopping = Event()
    for list_method, options in (
        (custom.list_cluster_custom_object, RESOURCE),
        (core.list_pod_for_all_namespaces, {"label_selector": POD_SELECTOR}),
    ):
        Thread(
            target=watch_changes, args=(list_method, changed, stopping),
            kwargs=options, daemon=True,
        ).start()

    LOG.info("COSC 3657 reconciliation controller started")
    try:
        # Watches only signal work. One loop applies changes from fresh API reads.
        while not stopping.is_set():
            changed.clear()
            reconcile_all(custom, core)
            changed.wait(RESYNC_SECONDS)
    except KeyboardInterrupt:
        LOG.info("Stopping controller")
    finally:
        stopping.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
