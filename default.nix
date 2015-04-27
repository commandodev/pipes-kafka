{ mkDerivation, base, bytestring, haskakafka, hedis, lens, mmorph
, mtl, pipes, pipes-concurrency, pipes-safe, stdenv, transformers
}:
mkDerivation {
  pname = "pipes-kafka";
  version = "0.1.0.0";
  src = ./.;
  buildDepends = [
    base bytestring haskakafka hedis lens mmorph mtl pipes
    pipes-concurrency pipes-safe transformers
  ];
  homepage = "https://github.com/boothead/pipes-kafka";
  description = "Kafka in the Pipes ecosystem";
  license = stdenv.lib.licenses.mit;
}
