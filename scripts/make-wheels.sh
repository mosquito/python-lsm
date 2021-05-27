set -ex

SRC=/app/src
DST=/app/dst

yum install -y file-devel

function build_wheel() {
	/opt/python/$1/bin/pip wheel ${SRC} -f ${SRC} -w ${DST}
}





cd ${DST}
for f in ./*linux_*; do if [ -f $f ]; then auditwheel repair $f -w . ; rm $f; fi; done
cd -

