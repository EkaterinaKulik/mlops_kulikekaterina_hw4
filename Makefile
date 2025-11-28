DBT_PROJECT_DIR = dbt
DBT = dbt

.PHONY: deps seed run test docs all

deps:
	cd $(DBT_PROJECT_DIR) && $(DBT) deps

seed:
	cd $(DBT_PROJECT_DIR) && $(DBT) seed

run:
	cd $(DBT_PROJECT_DIR) && $(DBT) run

test:
	cd $(DBT_PROJECT_DIR) && $(DBT) test

docs:
	cd $(DBT_PROJECT_DIR) && $(DBT) docs generate

all:
	cd $(DBT_PROJECT_DIR) && $(DBT) deps && $(DBT) seed && $(DBT) run && $(DBT) test
