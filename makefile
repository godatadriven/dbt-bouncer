build-pex:
	poetry run pex . \
		--interpreter-constraint ">=3.8,<3.13" \
		--jobs 128 \
		--max-install-jobs 0 \
		--output-file ./dist/dbt-bouncer.pex \
		--pip-version 23.2 \
		--platform macosx_11_0_x86_64-cp-38-cp38 \
		--platform macosx_11_0_x86_64-cp-39-cp39 \
		--platform macosx_11_0_x86_64-cp-310-cp310 \
		--platform macosx_11_0_x86_64-cp-311-cp311 \
		--platform macosx_11_0_x86_64-cp-312-cp312 \
		--platform manylinux2014_x86_64-cp-38-cp38 \
		--platform manylinux2014_x86_64-cp-39-cp39 \
		--platform manylinux2014_x86_64-cp-310-cp310 \
		--platform manylinux2014_x86_64-cp-311-cp311 \
		--platform manylinux2014_x86_64-cp-312-cp312 \
		--python-shebang='/usr/bin/env python' \
		--script dbt-bouncer

test:
	poetry run pytest \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=dbt_bouncer/ \
		--numprocesses 5 \
		./tests
