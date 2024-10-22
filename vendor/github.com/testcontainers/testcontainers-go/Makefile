include ./commons-test.mk

.PHONY: test-all
test-all: tools test-tools test-unit

.PHONY: test-examples
test-examples:
	@echo "Running example tests..."
	$(MAKE) -C examples test

.PHONY: tidy-all
tidy-all:
	$(MAKE) tidy
	$(MAKE) -C examples tidy-examples
	$(MAKE) -C modules tidy-modules

## --------------------------------------

TCENV=tcvenv
PYTHONBIN=./$(TCENV)/bin

tcvenv: tcvenv/touchfile

tcvenv/touchfile:
	@echo "Creating docs $(TCENV)..."
	test -d $(TCENV) || python3 -m venv $(TCENV)
	@echo "Installing requirements..."
	. $(PYTHONBIN)/activate; pip install -Ur requirements.txt
	touch $(TCENV)/touchfile

clean-docs:
	@echo "Destroying docs $(TCENV)..."
	rm -rf $(TCENV)

.PHONY: serve-docs
serve-docs: tcvenv
	. $(PYTHONBIN)/activate; $(PYTHONBIN)/mkdocs serve
