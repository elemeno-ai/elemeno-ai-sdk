.PHONY: clean pip-testpypi

clean:
	rm -rf dist

_pip-testpypi:
	python setup.py sdist bdist_wheel
	twine upload --repository testpypi dist/elemeno_ai_sdk-0.0.1-py2.py3-none-any.whl

pip-testpypi: clean _pip-testpypi

