.PHONY: clean docs pip-testpypi wheel

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -rf dist/
	rm -rf .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-env: ## remove dev environment
	rm -fr .direnv/

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '*.ipynb_checkpoints' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

env: 
	direnv allow
	pip install --upgrade pip
	pip install -e .

wheel:
	python setup.py bdist_wheel

docs:
	@MAKE html -C docs

_pip-testpypi: clean
	python setup.py sdist bdist_wheel
	twine upload --repository testpypi dist/*.whl

_pip-pypi: clean
	python setup.py sdist bdist_wheel
	twine upload --non-interactive dist/*.whl

_pip-pypi-elemeno: clean
	python setup.py sdist bdist_wheel
	twine upload --repository elemeno dist/*.whl

test:
	pytest

pip-testpypi: clean _pip-testpypi

pip-pypi: clean _pip-pypi

pip-pypi-elemeno: clean _pip-pypi-elemeno

bump-custom:
	bumpversion --new-version $(version) patch --verbose

bump-patch:
	bumpversion patch --tag --verbose
	@echo "New version: v$$(python setup.py --version)"
	@echo "Make sure to push the new tag to GitHub"

bump-minor:
	bumpversion minor --tag --verbose
	@echo "New version: v$$(python setup.py --version)"
	@echo "Make sure to push the new tag to GitHub"

bump-major:
	bumpversion major --tag --verbose
	@echo "New version: v$$(python setup.py --version)"
	@echo "Make sure to push the new tag to GitHub"

bump-dev:
	bumpversion build --tag --verbose
	@echo "New version: v$$(python setup.py --version)"
	@echo "Make sure to push the new tag to GitHub"