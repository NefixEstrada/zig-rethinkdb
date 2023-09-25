{
  description = "Zig Protobuf";
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    zig.url = "github:mitchellh/zig-overlay";
  };
  
  outputs = { self, nixpkgs, flake-utils, zig }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let pkgs = nixpkgs.legacyPackages.${system}; in
          {
            devShells.default = import ./shell.nix {
              inherit pkgs;
              zig = zig.packages.${system}.master;
            };
          }
        );
}
