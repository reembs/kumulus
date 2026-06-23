.SHELLFLAGS := -eu -o pipefail -c
SHELL := bash
ENV_FILE := .env.local

.FORCE:

deploy: .FORCE
	@if [[ -f "$(ENV_FILE)" ]]; then source "$(ENV_FILE)"; fi; \
	mvn -DskipTests clean package dokka:javadocJar deploy
