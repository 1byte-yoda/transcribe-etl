# Variable Declaration
PROJECT_FOLDER := transcribe_etl
TEST_TARGET = tests
FOLDERS_TO_CHECK := $(PROJECT_FOLDER) ${TEST_TARGET} main.py
KNOWN_TARGETS = cov_report
ARGS := $(filter-out $(KNOWN_TARGETS),$(MAKECMDGOALS))

# HTML Coverage Report
ifeq ($(ARGS), html)
	COV_REPORT_TYPE := --cov-report html
endif

# XML Coverage Report
ifeq ($(ARGS), xml)
	COV_REPORT_TYPE := --cov-report xml
endif

PYTHON=python3.9

# Check for Type Hint inconsistencies
.PHONY: typehint
typehint:
	mypy --ignore-missing-imports $(FOLDERS_TO_CHECK)

# Run all Test Suites under the tests folder
.PHONY: test
test:
	 PYTHONPATH=. pytest ${TEST_TARGET} -v -s

# Format the code into black formatting
.PHONY: black
black:
	black -l 180 $(FOLDERS_TO_CHECK)

# Check for Lint errors
.PHONY: lint
lint:
	flake8 $(FOLDERS_TO_CHECK)

# Check for Security Vulnerabilities
.PHONY: scan_security
scan_security:
	bandit $(FOLDERS_TO_CHECK)

# Clean up local development's cache data
.PHONY: clean
clean:
	find . -type f -name "*.pyc" | xargs rm -fr
	find . -type d -name __pycache__ | xargs rm -fr
	find . -type d -name .mypy_cache | xargs rm -fr
	find . -type d -name .pytest_cache | xargs rm -fr

# Run all Pre-commit Checks
.PHONY: checklist
checklist: black lint cov_report_cobertura clean

# Check Coverage Report
.DEFAULT: ;: do nothing

.SUFFIXES:
.PHONY: cov_report
cov_report:
	PYTHONPATH=. pytest --cov-config=.coveragerc --cov=${PROJECT_FOLDER} $(COV_REPORT_TYPE)


.SUFFIXES:
.PHONY: cov_report_cobertura
cov_report_cobertura:
	coverage erase
	pytest --cov=${PROJECT_FOLDER} -s -v
	coverage report --skip-empty
	coverage xml
