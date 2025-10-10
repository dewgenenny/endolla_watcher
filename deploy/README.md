# Endolla Watcher Kubernetes deployment

The manifests in this directory describe the Kubernetes resources required to run
Endolla Watcher with distinct backend and frontend workloads. They are packaged
as a plain Kustomize base that can be consumed directly by Argo CD or extended
with environment-specific overlays.

## Components

* `namespace.yaml` &ndash; creates the dedicated `endolla-watcher` namespace.
* `pvc.yaml` &ndash; persistent volume claim that stores the SQLite database used by the backend.
* `backend-configmap.yaml` &ndash; configuration for the backend container, including fetch intervals and rule thresholds.
* `backend-deployment.yaml` &ndash; FastAPI backend deployment responsible for ingesting data and serving the JSON API.
* `backend-service.yaml` &ndash; internal service exposing the backend on port `8000`.
* `frontend-deployment.yaml` &ndash; static site served via NGINX which consumes the backend API.
* `frontend-service.yaml` &ndash; ClusterIP service exposing the frontend on port `80`.
* `kustomization.yaml` &ndash; ties the manifests together for Kustomize/Argo CD.

## Deploying with Argo CD

Point an Argo CD `Application` at the `deploy/` directory or apply the example
under `argocd/application.yaml`. Update the container image references if your
GHCR repository differs from the defaults and adjust the ConfigMap with any
custom rule values or fetch intervals you require. The GitHub Actions workflow
automatically rewrites the Kustomize image tags to the commit SHA that produced
each container, ensuring Argo CD always detects a manifest change when new
images are published.

Once synced, Argo CD will create the namespace, persistent volume claim,
backend API deployment and the static frontend. Configure your ingress or
service mesh to route `/api` traffic to the backend service and everything else
to the frontend service.
