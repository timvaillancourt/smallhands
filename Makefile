all:
	pip install -r requirements.txt

run:
	$(PWD)/smallhands.py -c config.yml

test:
	flake8 --ignore E501,E221
