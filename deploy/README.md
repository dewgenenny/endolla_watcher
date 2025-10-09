# Endolla Watcher Kubernetes deployment

The manifests in this directory package the Endolla Watcher container for
Kubernetes and are designed to be managed by Argo CD. They are structured as a
plain Kustomize base so they can be consumed directly by Argo CD or further
customised with environment-specific overlays.

## Components

* `namespace.yaml` – creates the dedicated `endolla-watcher` namespace.
* `configmap.yaml` – stores non-sensitive Git configuration used when pushing
  the generated site.
* `secret.yaml` – placeholder secret that must be updated with a valid
  `GH_TOKEN` value before deploying.
* `pvc.yaml` – persistent volume claim that holds the SQLite database and the
  rendered static site content.
* `deployment.yaml` – runs the watcher container with sensible defaults and the
  required volume mount. Update the container image reference if your GitHub
  organisation or username differs from the default `ghcr.io/<owner>/endolla-watcher`.
* `kustomization.yaml` – ties the manifests together for use with Kustomize.

## Preparing secrets

Update `secret.yaml` so that `stringData.GH_TOKEN` contains a GitHub token with
permission to push to the repository configured in `configmap.yaml`. If you do
not wish to commit the secret to source control you can instead delete the file
from the kustomization and create the secret manually:

```bash
kubectl create secret generic endolla-watcher-secrets \
  --namespace endolla-watcher \
  --from-literal GH_TOKEN="<your token>"
```

Remove the `secret.yaml` entry from `kustomization.yaml` if you follow the
manual approach.

## Deploying with Argo CD

Point an Argo CD `Application` at the `deploy/` directory. The repository also
includes an example application manifest under `argocd/application.yaml` that
can be applied to an Argo CD control plane. Adjust the Git repository URL and
any required configuration before applying it:

```bash
kubectl apply -f argocd/application.yaml
```

Once synced, Argo CD will create the namespace, persistent volume claim, secret
and deployment required to run Endolla Watcher in Kubernetes. The generated site
and SQLite database will be stored on the persistent volume mounted at
`/data` inside the container.
