all:
	pip install -r requirements.txt

run:
	$(PWD)/smallhands.py -c config.yml
