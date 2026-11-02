TF := terraform -chdir=terraform

.PHONY: test lint init plan deploy demo demo-new-metric outputs destroy

test:            ## Run unit tests for the transform Lambda
	python -m pytest -q

lint:            ## Format-check Terraform and validate
	$(TF) fmt -check -recursive
	$(TF) init -backend=false -input=false >/dev/null
	$(TF) validate

init:
	$(TF) init

plan: init
	$(TF) plan

deploy: init     ## Create the whole pipeline
	$(TF) apply

demo:            ## Publish demo app metrics for 10 minutes
	python scripts/generate_metrics.py --minutes 10

demo-new-metric: ## Publish a brand-new metric name to prove auto-inclusion
	python scripts/generate_metrics.py --minutes 5 --new-metric

outputs:
	$(TF) output

destroy:         ## Tear everything down
	$(TF) destroy