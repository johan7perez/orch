fn main() {
    // `libduckdb-sys` usa la Restart Manager de Windows para poder decir qué
    // proceso tiene bloqueado el fichero de base de datos, pero su script de
    // compilación no enlaza la librería que la define. Sin esto, el enlazado
    // falla con `RmStartSession` y compañía sin resolver.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=dylib=rstrtmgr");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
