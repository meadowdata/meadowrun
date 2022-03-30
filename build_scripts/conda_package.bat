@REM takes the latest package uploaded to

@REM unlike the other scripts in this directory, run this from the build_scripts directory so that cleaning up is easy

rmdir /S /Q meadowrun

@REM to use test.pypi.org, you can replace this with:
@REM call conda skeleton pypi --pypi-url https://test.pypi.io/pypi/ meadowrun
call conda skeleton pypi meadowrun

echo Now add open meadowrun/meta.yaml and add "poetry" under requirements: host:
pause

call conda build -c defaults -c conda-forge --python 3.9 meadowrun
