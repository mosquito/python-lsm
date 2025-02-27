set -ex

mkdir -p dist

function build_wheel() {
	/opt/python/$1/bin/pip wheel . -f . -w dist
}

build_wheel cp39-cp39
build_wheel cp310-cp310
build_wheel cp311-cp311
build_wheel cp312-cp312
build_wheel cp313-cp313

cd dist
for f in ./*linux_*;
do if [ -f $f ]; then auditwheel repair $f -w . ; rm $f; fi;
done
cd -
