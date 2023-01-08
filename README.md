


### CMD
#### server
$ go run main.go

#### client
curl -X POST localhost:8080 -d \
  '{"record": {"value": "TGV0J3MgR28gIzEK"}}'
curl -X POST localhost:8080 -d \
  '{"record": {"value": "TGV0J3MgR28gIzIK"}}'
curl -X POST localhost:8080 -d \
  '{"record": {"value": "TGV0J3MgR28gIzMK"}}'


curl -X GET localhost:8080 -d '{"offset": 0}'
curl -X GET localhost:8080 -d '{"offset": 1}'
curl -X GET localhost:8080 -d '{"offset": 2}'

# Deploy to Kind

```
Deploy
$ kind create cluster
$ make build-docker
$ kind load docker-image github.com/yurakawa/proglog:0.0.1
$ helm install proglog deploy/proglog
$ kubectl relay host/proglog-0.proglog.default.svc.cluster.local 8400

Delete
$ helm delete proglog
```

### Send Request 

```
$ go run cmd/getservers/main.go
```
