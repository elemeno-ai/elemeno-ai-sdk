.PHONY: clean pip-testpypi wheel

clean:
	rm -rf dist

wheel:
	python setup.py bdist_wheel

_pip-testpypi: clean
	python setup.py sdist bdist_wheel
	twine upload --repository testpypi dist/*.whl

_pip-pypi: clean
	python setup.py sdist bdist_wheel
	twine upload dist/*.whl

pip-testpypi: clean _pip-testpypi

pip-pypi: clean _pip-pypi

bump:
	python -m bumpversion --new-version 0.0.12 patch --verbose
