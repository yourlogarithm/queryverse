fn main() {
    prost_build::compile_protos(&["../shared/vector.proto"], &["../shared"]).unwrap();
}
