protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/sardine.proto

cd server/alloc && go build -o sardine-alloc
cd ../..

cd server/assign && go build -o sardine-assign
cd ../..

cd server/proxy && go build -o sardine-proxy
cd ../..
