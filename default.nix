{ mkDerivation, base, bytestring, exceptions, hw-kafka-client
, monad-logger, pipes, pipes-safe, stdenv, text, transformers
, transformers-base
}:
mkDerivation {
  pname = "pipes-kafka";
  version = "0.1.0.0";
  src = ./.;
  libraryHaskellDepends = [
    base bytestring exceptions hw-kafka-client monad-logger pipes
    pipes-safe text transformers transformers-base
  ];
  homepage = "https://github.com/boothead/pipes-kafka";
  description = "Kafka in the Pipes ecosystem";
  license = stdenv.lib.licenses.mit;
}
