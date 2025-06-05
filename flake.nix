# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under both the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree and the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree.
{
  description = "A flake for hacking on and building buck2";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ (import rust-overlay) ];
      };

      buck2BuildInputs = [
        pkgs.bashNonInteractive
        pkgs.cacert
        pkgs.clang_16
        pkgs.coreutils
        pkgs.curl
        pkgs.git
        pkgs.gnupg
        pkgs.gnused
        pkgs.gnutar
        pkgs.gzip
        pkgs.lld_16
        pkgs.mold-wrapped
        pkgs.python3
        pkgs.unzip
        pkgs.wget
      ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
        pkgs.stdenv.cc.bintools
        pkgs.darwin.cctools
      ];

    rust-version = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain;
    my-rust-bin = rust-version.override {
      extensions = [ "rust-analyzer" "rust-src" ];
    };

    in {
      apps.dockerBuild =
        { program = "${self.packages.${system}.dockerImage}"; type = "app"; };

      packages = {
        # protoc is downloaded and run during the build but it is dynamically linked and
        # does not work on NixOS; make it work by using nix-ld
        baseImage = pkgs.dockerTools.buildImage {
            name = "base-ld";
            tag = "latest";

            config = {
              Env = [
                # ensure libstdc++.so is found
                "NIX_LD_LIBRARY_PATH=${pkgs.stdenv.cc.cc.lib}/lib"
                "NIX_LD=/share/nix-ld/lib/ld.so"
              ];
              Label = [
                # connect github repo to Github package
                #"org.opencontainers.image.source=https://github.com/owner/repo"
              ];
            };

            copyToRoot = pkgs.buildEnv {
                name = "ld-library-path";
                pathsToLink = [ "/lib" ];
                paths = map pkgs.lib.getLib [];
                postBuild = ''
                  ln -s ${pkgs.stdenv.cc.bintools.dynamicLinker} $out/share/nix-ld/lib/ld.so
                '';
                extraPrefix = "/share/nix-ld";
                ignoreCollisions = true;
            };

            runAsRoot = ''
              #!${pkgs.runtimeShell}
              mkdir -p /lib64
              ln -s ${pkgs.nix-ld}/bin/nix-ld /lib64/ld-linux-x86-64.so.2
            '';
        };

        dockerImage =
          let
            inherit (pkgs) dockerTools python3;

            image = dockerTools.streamNixShellImage {
              name = "nix-build";
              drv = pkgs.mkShell.override { stdenv = pkgs.stdenvNoCC; }
                {
                    PATH = pkgs.lib.makeBinPath (buck2BuildInputs ++ [my-rust-bin]);

                    nativeBuildInputs = [ python3 ];
                };
              shell = pkgs.lib.getExe pkgs.bashNonInteractive;
              tag = "latest";
            };
          in
            image.override (
              pkgs.lib.optionalAttrs pkgs.stdenv.isLinux { fromImage = self.packages.${system}.baseImage; }
            );
      };

      devShells.default = pkgs.mkShell {
        buildInputs = pkgs.lib.optionals pkgs.stdenv.isDarwin (with pkgs.darwin.apple_sdk.frameworks; [
          CoreFoundation
          CoreServices
          IOKit
          Security
        ]);
        packages = [ pkgs.cargo-bloat my-rust-bin pkgs.mold-wrapped pkgs.reindeer pkgs.lld_16 pkgs.clang_16 ];
        shellHook =
          ''
            export BUCK2_BUILD_PROTOC=${pkgs.protobuf}/bin/protoc
            export BUCK2_BUILD_PROTOC_INCLUDE=${pkgs.protobuf}/include
          ''
          # enable mold for linux users, for more tolerable link times
          # we have to specify tokio_unstable in the RUSTFLAGS here since they override
          # .cargo/config.toml that is the reasonable place to specify it
          + pkgs.lib.optionalString pkgs.stdenv.isLinux ''
            export RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold --cfg=tokio_unstable $RUSTFLAGS"
          '';
      };
    });
}
