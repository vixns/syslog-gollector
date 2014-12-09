binary_name=syslog-gollector
docker_registry_account=emmanuel
docker_tag=latest

build/binary: *.go
	GOOS=linux GOARCH=amd64 go build -o build/$(binary_name)

stage/binary: build/$(binary_name)
	mkdir -p stage
	cp build/$(binary_name) stage/$(binary_name)

build/container: stage/$(binary_name) Dockerfile
	docker build --no-cache -t syslog-gollector .
	touch build/container

release:
	docker tag syslog-gollector $(docker_registry_account)/syslog-gollector:$(docker_tag)
	docker push $(docker_registry_account)/syslog-gollector:$(docker_tag)

.PHONY: clean
clean:
	rm -rf {build,stage}
