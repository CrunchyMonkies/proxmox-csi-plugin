REGISTRY ?= ghcr.io
USERNAME ?= sergelogvinov
OCIREPO ?= $(REGISTRY)/$(USERNAME)
HELMREPO ?= $(REGISTRY)/$(USERNAME)/charts
PLATFORM ?= linux/arm64,linux/amd64
PUSH ?= false

SHA ?= $(shell git describe --match=none --always --abbrev=7 --dirty)
TAG ?= $(shell git describe --tag --always --match v[0-9]\*)
GO_LDFLAGS := -ldflags "-w -s -X main.version=$(TAG) -X main.commit=$(SHA)"

OS ?= $(shell go env GOOS)
ARCH ?= $(shell go env GOARCH)
ARCHS ?= amd64 arm64

BUILD_ARGS := --platform=$(PLATFORM)
ifeq ($(PUSH),true)
BUILD_ARGS += --push=$(PUSH)
BUILD_ARGS += --output type=image,annotation-index.org.opencontainers.image.source="https://github.com/$(USERNAME)/proxmox-csi-plugin",annotation-index.org.opencontainers.image.description="Proxmox VE CSI plugin"
else
BUILD_ARGS += --output type=docker
endif

COSING_ARGS ?=

############

# Help Menu

define HELP_MENU_HEADER
# Getting Started

To build this project, you must have the following installed:

- git
- make
- golang 1.20+
- golangci-lint

endef

export HELP_MENU_HEADER

help: ## This help menu
	@echo "$$HELP_MENU_HEADER"
	@grep -E '^[a-zA-Z0-9%_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

############
#
# Build Abstractions
#

build-all-archs:
	@for arch in $(ARCHS); do $(MAKE) ARCH=$${arch} build ; done

.PHONY: clean
clean: ## Clean
	rm -rf bin .cache

.PHONY: tools
tools:
	go install github.com/google/go-licenses@latest

build-pvecsictl:
	CGO_ENABLED=0 GOOS=$(OS) GOARCH=$(ARCH) go build $(GO_LDFLAGS) \
		-o bin/pvecsictl-$(ARCH) ./cmd/pvecsictl

build-volume-operator:
	CGO_ENABLED=0 GOOS=$(OS) GOARCH=$(ARCH) go build $(GO_LDFLAGS) \
		-o bin/proxmox-csi-operator-$(ARCH) ./cmd/volume-operator

build-%:
	CGO_ENABLED=0 GOOS=$(OS) GOARCH=$(ARCH) go build $(GO_LDFLAGS) \
		-o bin/proxmox-csi-$*-$(ARCH) ./cmd/$*

.PHONY: build
build: build-controller build-node build-pvecsictl build-volume-operator ## Build

.PHONY: run
run: build-controller ## Run
	go run $(GO_LDFLAGS) -race ./cmd/controller/main.go --cloud-config=hack/cloud-config.yaml -v=5 --metrics-address=:8080
	# ./bin/proxmox-csi-controller-$(ARCH) --cloud-config=hack/cloud-config.yaml -v=5 --metrics-address=:8080

.PHONY: lint
lint: ## Lint Code
	golangci-lint run --config .golangci.yml

.PHONY: unit
unit: ## Unit Tests
	go test -tags=unit $(shell go list ./...) $(TESTARGS)

.PHONY: test
test: lint unit ## Run all tests

.PHONY: licenses
licenses:
	go-licenses check ./... --disallowed_types=forbidden,restricted,unknown

# The volume operator holds a management-cluster credential, so the CSI binaries
# must not link it (or controller-runtime) in. Keeping the credential path inside
# cmd/volume-operator and pkg/operator is what makes it auditable on its own.
.PHONY: operator-isolation
operator-isolation: ## Fail if the CSI binaries import the volume operator
	@out="$$(go list -deps ./cmd/controller ./cmd/node ./cmd/pvecsictl | \
		grep -E '^(sigs.k8s.io/controller-runtime|github.com/sergelogvinov/proxmox-csi-plugin/pkg/operator)(/|$$)')"; \
		test -z "$$out" || { echo "CSI binaries import operator code:"; echo "$$out"; exit 1; }

.PHONY: conformance
conformance: ## Conformance
	docker run --rm -it -v $(PWD):/src -w /src ghcr.io/siderolabs/conform:v0.1.0-alpha.31 enforce

############
#
# Code generation (volume operator)
#
# controller-gen's version is pinned by the go.mod `tool` directive, so CI cannot
# drift from a developer's local version.

OPERATOR_CHART := charts/proxmox-csi-plugin

.PHONY: generate
generate: ## Generate deepcopy methods for the operator API types
	go tool controller-gen object:headerFile=hack/boilerplate.go.txt paths=./pkg/apis/...

.PHONY: manifests
manifests: ## Generate the operator CRDs and ClusterRole into the chart
	go tool controller-gen crd paths=./pkg/apis/... output:crd:artifacts:config=$(OPERATOR_CHART)/files/crds
	# The cluster-scoped half of the operator's permissions is generated from the
	# +kubebuilder:rbac markers next to the controllers that need it, so the chart
	# cannot grant more than the code asked for. The namespaced half -- leases,
	# events, the credential Secret -- is hand-written in operator-role.yaml,
	# because controller-gen would fold it into this same ClusterRole and hand the
	# operator cluster-wide Secret reads.
	go tool controller-gen rbac:roleName=proxmox-csi-operator paths=./pkg/operator/... \
		output:rbac:stdout > $(OPERATOR_CHART)/files/operator-role.yaml

.PHONY: proto
proto: ## Generate Go code from protobuf definitions
	go tool buf generate

# --porcelain rather than `git diff`, because newly generated files are untracked
# and a plain diff would not see them.
.PHONY: generate-check
generate-check: generate manifests proto ## Fail if generated output is stale or uncommitted
	@out="$$(git status --porcelain -- pkg/apis $(OPERATOR_CHART)/files)"; \
		test -z "$$out" || { \
			echo "generated output is stale or uncommitted, run 'make generate manifests proto':"; \
			echo "$$out"; exit 1; }

############

.PHONY: helm-unit
helm-unit: ## Helm Unit Tests
	@helm lint charts/proxmox-csi-plugin
	@for values in charts/proxmox-csi-plugin/ci/*values.yaml; do \
		echo "helm template $$values"; \
		helm template -f "$$values" proxmox-csi-plugin charts/proxmox-csi-plugin >/dev/null || exit 1; \
	done

.PHONY: helm-login
helm-login: ## Helm Login
	@echo "${HELM_TOKEN}" | helm registry login $(REGISTRY) --username $(USERNAME) --password-stdin

.PHONY: helm-release
helm-release: ## Helm Release
	@rm -rf dist/
	@helm package charts/proxmox-csi-plugin -d dist
	@helm push dist/proxmox-csi-plugin-*.tgz oci://$(HELMREPO) 2>&1 | tee dist/.digest
	@cosign sign --yes $(COSING_ARGS) $(HELMREPO)/proxmox-csi-plugin@$$(cat dist/.digest | awk -F "[, ]+" '/Digest/{print $$NF}')

############

.PHONY: docs
docs:
	helm version
	yq -i '.appVersion = "$(TAG)"' charts/proxmox-csi-plugin/Chart.yaml
	helm template -n csi-proxmox proxmox-csi-plugin \
		-f charts/proxmox-csi-plugin/values.edge.yaml \
		charts/proxmox-csi-plugin > docs/deploy/proxmox-csi-plugin.yml
	helm template -n csi-proxmox proxmox-csi-plugin \
		--set-string image.tag=$(TAG) \
		--set createNamespace=true \
		charts/proxmox-csi-plugin > docs/deploy/proxmox-csi-plugin-release.yml
	helm template -n csi-proxmox proxmox-csi-plugin \
		-f charts/proxmox-csi-plugin/values.talos.yaml \
		--set-string image.tag=$(TAG) \
		charts/proxmox-csi-plugin > docs/deploy/proxmox-csi-plugin-talos.yml
	helm-docs --sort-values-order=file charts/proxmox-csi-plugin

release-update:
	git-chglog --config hack/chglog-config.yml -o CHANGELOG.md

############
#
# Docker Abstractions
#

.PHONY: docker-init
docker-init:
	docker run --rm --privileged multiarch/qemu-user-static:register --reset

	docker context create multiarch ||:
	docker buildx create --name multiarch --driver docker-container --use ||:
	docker context use multiarch
	docker buildx inspect --bootstrap multiarch

image-%:
	docker buildx build $(BUILD_ARGS) \
		--build-arg TAG=$(TAG) \
		--build-arg SHA=$(SHA) \
		-t $(OCIREPO)/$*:$(TAG) \
		--target $* \
		-f Dockerfile .

.PHONY: images-checks
images-checks: images image-tools-check
	trivy image --exit-code 1 --ignore-unfixed --severity HIGH,CRITICAL --no-progress $(OCIREPO)/proxmox-csi-controller:$(TAG)
	trivy image --exit-code 1 --ignore-unfixed --severity HIGH,CRITICAL --no-progress $(OCIREPO)/proxmox-csi-node:$(TAG)
	trivy image --exit-code 1 --ignore-unfixed --severity HIGH,CRITICAL --no-progress $(OCIREPO)/pvecsictl:$(TAG)
	trivy image --exit-code 1 --ignore-unfixed --severity HIGH,CRITICAL --no-progress $(OCIREPO)/proxmox-csi-operator:$(TAG)

.PHONY: images-cosign
images-cosign:
	@cosign sign --yes $(COSING_ARGS) --recursive $(OCIREPO)/proxmox-csi-controller:$(TAG)
	@cosign sign --yes $(COSING_ARGS) --recursive $(OCIREPO)/proxmox-csi-node:$(TAG)
	@cosign sign --yes $(COSING_ARGS) --recursive $(OCIREPO)/pvecsictl:$(TAG)
	@cosign sign --yes $(COSING_ARGS) --recursive $(OCIREPO)/proxmox-csi-operator:$(TAG)

.PHONY: images
images: image-proxmox-csi-controller image-proxmox-csi-node image-pvecsictl image-proxmox-csi-operator ## Build images
