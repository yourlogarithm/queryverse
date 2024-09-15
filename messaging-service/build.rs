fn main() {
    tonic_build::compile_protos("../proto/messaging.proto").unwrap();
}