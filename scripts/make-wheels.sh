set -ex

mkdir -p ${WHEEL_DST}

function build_wheel() {
	/opt/python/$1/bin/pip wheel ${WHEEL_SRC} -f ${WHEEL_SRC} -w ${WHEEL_DST}
}

build_wheel cp35-cp35m
build_wheel cp36-cp36m
build_wheel cp37-cp37m
build_wheel cp38-cp38
build_wheel cp39-cp39

cd ${WHEEL_DST}
for f in ./*linux_*; do if [ -f $f ]; then auditwheel repair $f -w . ; rm $f; fi; done
cd -
