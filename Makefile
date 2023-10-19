.PHONY: clean docs pip-pypi wheel

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -rf dist/
	rm -rf build/
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
	pip install poetry 
	poetry install

requirements:
	poetry export --output requirements.txt --without-hashes --without-urls

requirements-dev:
	poetry export --output requirements-dev.txt --only dev --without-hashes --without-urls

docs:
	@MAKE html -C docs

_pip-pypi: clean requirements
	python setup.py sdist bdist_wheel
	twine upload --non-interactive dist/*.whl

test:
	poetry run pytest

lint/flake8: ## check style with flake8
	poetry run flake8 .

lint/isort: ## check import style with isort
	poetry run isort .

lint/black: ## check style with black
	poetry run black .

lint: lint/flake8 lint/isort lint/black ## check style

pip-pypi: clean _pip-pypi

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

bump-preminor:
	bumpversion minor --verbose
	bumpversion prerelease --tag --verbose
	@echo "New version: v$$(python setup.py --version)"
	@echo "Make sure to push the new tag to GitHub"

bump-prepatch:
	bumpversion patch --verbose
	bumpversion prerelease --tag --verbose
	@echo "New version: v$$(python setup.py --version)"
	@echo "Make sure to push the new tag to GitHub"