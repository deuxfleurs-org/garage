DOCKER=lxpz/garage_builder_amd64

docker:
	docker build -t $(DOCKER):$(TAG) .
	docker push $(DOCKER):$(TAG)
	docker tag $(DOCKER):$(TAG) $(DOCKER):latest
	docker push $(DOCKER):latest
	
