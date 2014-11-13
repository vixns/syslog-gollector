build/container: stage/syslog-gollector Dockerfile
	docker build --no-cache -t syslog-gollector .
	touch build/container

build/syslog-gollector: *.go
	GOOS=linux GOARCH=amd64 go build -o build/syslog-gollector

stage/syslog-gollector: build/syslog-gollector
	mkdir -p stage
	cp build/syslog-gollector stage/syslog-gollector

release:
	docker tag syslog-gollector emmanuel/syslog-gollector
	docker push emmanuel/syslog-gollector

.PHONY: clean
clean:
	rm -rf build
