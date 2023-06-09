{
  description = "A dev environment for Fly.io Gossip Glomers challenge";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };


  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
          };

          maelstrom = pkgs.stdenv.mkDerivation rec{
            name = "Maelstrom";
            version = "0.2.3";

            src = builtins.fetchTarball {
              url = "https://github.com/jepsen-io/maelstrom/releases/download/v${version}/maelstrom.tar.bz2";
              sha256 = "sha256:1hkczlbgps3sl4mh6hk49jimp6wmks8hki0bqijxsqfbf0hcakwq";
            };

            # include upstream patch (not yet in 0.2.3)
            # https://github.com/jepsen-io/maelstrom/commit/f0ce6dbc60369ec85f8cd69aa5ac953806634260
            maelstromScriptPatch = pkgs.writeShellScript "maelstrom" ''
              # A small wrapper script for invoking the Maelstrom jar, with arguments.
              SCRIPT_DIR=$( cd -- "$( dirname "$(readlink -f "''${BASH_SOURCE[0]}")" )" &> /dev/null && pwd )
              exec java -Djava.awt.headless=true -jar "''${SCRIPT_DIR}/lib/maelstrom.jar" "$@"
            '';

            installPhase = ''
              mkdir -p $out/bin
              cp -r * $out/bin/
              cp $maelstromScriptPatch $out/bin/maelstrom
            '';

          };

        in
        {
          devShell = pkgs.mkShell {
            name = "Gossip Glomers env";

            buildInputs = with pkgs; [
              maelstrom
              jdk
              graphviz
              gnuplot

              # Golang
              go
              gopls
              delve
              golint
            ];

          };
        });
}
