GO ?= go
PORT_A ?= 8031
PORT_B ?= 8032

.PHONY: multiproc
multiproc:
	@set -eu; \
	demo_dir=$$(mktemp -d); a=; b=; \
	cleanup() { \
		for pid in "$$a" "$$b"; do \
			if [ -n "$$pid" ]; then kill "$$pid" 2>/dev/null || true; fi; \
		done; \
		wait || true; \
		rm -f "$$demo_dir/webdemo"; \
		rmdir "$$demo_dir"; \
	}; \
	trap cleanup EXIT; \
	trap 'exit 130' INT; \
	trap 'exit 143' TERM; \
	GOWORK=off $(GO) build -o "$$demo_dir/webdemo" .; \
	printf 'Process A: http://localhost:$(PORT_A)\nProcess B: http://localhost:$(PORT_B)\nCtrl-C stops both processes.\n'; \
	"$$demo_dir/webdemo" -listen :$(PORT_A) -peer-id a -peer-url http://localhost:$(PORT_B) & a=$$!; \
	"$$demo_dir/webdemo" -listen :$(PORT_B) -peer-id b -peer-url http://localhost:$(PORT_A) & b=$$!; \
	while kill -0 "$$a" 2>/dev/null && kill -0 "$$b" 2>/dev/null; do sleep 1; done; \
	if ! kill -0 "$$a" 2>/dev/null; then wait "$$a"; fi; \
	if ! kill -0 "$$b" 2>/dev/null; then wait "$$b"; fi
