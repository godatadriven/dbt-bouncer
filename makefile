build-pex:
	poetry run pex . \
		-c dbt-bouncer \
		-o ./dist/dbt-bouncer.pex \
		--python-shebang='/usr/bin/env python' \
		--platform macosx_11_0_x86_64-cp-38-cp38 \
		--platform macosx_11_0_x86_64-cp-39-cp39 \
		--platform macosx_11_0_x86_64-cp-310-cp310 \
		--platform macosx_11_0_x86_64-cp-311-cp311 \
		--platform macosx_11_0_x86_64-cp-312-cp312 \
		--platform manylinux2014_x86_64-cp-38-cp38 \
		--platform manylinux2014_x86_64-cp-39-cp39 \
		--platform manylinux2014_x86_64-cp-310-cp310 \
		--platform manylinux2014_x86_64-cp-311-cp311 \
		--platform manylinux2014_x86_64-cp-312-cp312

test:
	poetry run pytest \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=dbt_bouncer/ \
		--numprocesses 5 \
		./tests
