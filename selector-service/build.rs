fn main() {
    tonic_build::compile_protos("../proto/crawler.proto").unwrap();
    tonic_build::compile_protos("../proto/messaging.proto").unwrap();
}
