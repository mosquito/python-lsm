build: sdist

sdist:
	python3 setup.py sdist

linux-wheels:
	docker run --rm -v $(PWD):/mnt -w /mnt quay.io/pypa/manylinux_2_34_x86_64 bash /mnt/scripts/make-wheels.sh
