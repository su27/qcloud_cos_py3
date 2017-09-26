.PHONY: test test-coverage publish

test:
	py.test

test-coverage:
	py.test --cov=qcloud_cos_py3 --cov-report html tests/

publish:
	rm -rf dist && python setup.py sdist upload -r pypi
