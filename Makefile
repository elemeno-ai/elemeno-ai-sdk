.PHONY: clean pip-testpypi wheel

clean:
	rm -rf dist build

wheel:
	python setup.py bdist_wheel

_pip-testpypi: clean
	python setup.py sdist bdist_wheel
	twine upload --repository testpypi dist/*.whl

_pip-pypi: clean
	python setup.py sdist bdist_wheel
	twine upload dist/*.whl

_pip-pypi-elemeno: clean
	python setup.py sdist bdist_wheel
	twine upload --repository elemeno dist/*.whl

pip-testpypi: clean _pip-testpypi

pip-pypi: clean _pip-pypi

pip-pypi-elemeno: clean _pip-pypi-elemeno

bump:
	python -m bumpversion --new-version $(version) patch --verbose
