fn main() {
    tonic_build::compile_protos("../proto/tei.proto").unwrap();
}
