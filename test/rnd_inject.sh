#!/bin/sh

BUNDLE=$(bp7 rnd 2>&1| grep 9f)

#RUST_LOG=debug cargo run -- -v $BUNDLE
#RUST_LOG=info cargo run -- -v $BUNDLE
cargo run -- -v add --hex $BUNDLE

echo "Bundle Dest / Source: "
bp7 decode $BUNDLE 2>&1 | grep DtnAddress -A1 | grep "//" | head -n2
