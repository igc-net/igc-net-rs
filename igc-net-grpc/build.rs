fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &["../../specs/proto/igc_net_v0.proto"],
            &["../../specs/proto/"],
        )?;
    Ok(())
}
