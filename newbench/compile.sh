pushd `dirname $BASH_SOURCE`

echo Compiling scala programs, target jvm 1.5 # compatible with jad.
rm -rf bin
mkdir -p bin
scalac -target:jvm-1.5 -d bin src/**/*.scala

popd