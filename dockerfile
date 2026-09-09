FROM uaes-acr-registry-vpc.cn-shanghai.cr.aliyuncs.com/api-gateway/olr-realtime-env:1.0
COPY . /opt/openlogreplicator
RUN cd /opt/openlogreplicator/proto; /opt/protobuf/bin/protoc  OraProtoBuf.proto --cpp_out=. ; mv OraProtoBuf.pb.cc ../src/common/OraProtoBuf.pb.cpp; mv OraProtoBuf.pb.h ../src/common/OraProtoBuf.pb.h; cd .. ; mkdir cmake-build-release-amd64; cd cmake-build-release-amd64; cmake ${BUILDARGS}; cmake --build ./ --target OpenLogReplicator -j ;

CMD tail -f