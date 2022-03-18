REM run once:
REM poetry config repositories.testpypi https://test.pypi.org/legacy/

call poetry build
call poetry publish -r testpypi
