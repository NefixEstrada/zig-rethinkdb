{ pkgs ? import <nixpkgs> {}, zig }:
  pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
      zig
      pkg-config
      postgresql_15
    ];
}
