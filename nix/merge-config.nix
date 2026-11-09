{
  writeShellApplication,
  jq,
  coreutils,
}:

writeShellApplication {
  name = "cloudreve-client-merge-config";
  runtimeInputs = [
    jq
    coreutils
  ];
  text = builtins.readFile ./merge-config.sh;
  meta.description = "Merge Nix-managed settings into ~/.cloudreve/config.json";
}
