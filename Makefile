.PHONY: test test-coverage

test:
	python -m unittest discover

test-coverage:
	coverage run -m unittest discover && coverage report && coverage html

publish:
	rm dist/* && python setup.py sdist
