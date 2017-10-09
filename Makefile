DOCKER_TAG?=smallhands
VERSION?=$(shell cat VERSION)

all:
	pip install -r requirements.txt

run:
	$(PWD)/smallhands.py -c config.yml

docker:
	docker build -t $(DOCKER_TAG):$(VERSION) .
	docker tag $(DOCKER_TAG):$(VERSION) $(DOCKER_TAG):latest

docker-push:
	docker push $(DOCKER_TAG):$(VERSION)
	docker push $(DOCKER_TAG):latest

test:
	flake8 --ignore E501,E221
