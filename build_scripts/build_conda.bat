REM Before running this file, update meta.yaml to be in sync with pyproject.toml, see
REM comments in that file to see what needs to be synced from pyproject.toml. Also
REM update the version numbers in this file

REM Build the pip package
poetry build

REM Unpack the resulting meadowrun-<version>.tar.gz file into /dist/meadowrun-<version>
tar xzvf dist\meadowrun-0.2.0.tar.gz -C dist

REM Build the conda package
call conda build -c defaults -c conda-forge --python 3.9 build_scripts\conda-recipe

REM To test this, you can install this package into a conda environment by running
REM `conda install meadowrun -c c:\bin\Miniconda\conda-bld -c defaults -c conda-forge`
REM (obviously replace c:\bin\Miniconda with your conda installation)

REM To publish this image, run
REM `conda activate base && anaconda upload C:\bin\Miniconda\conda-bld\noarch\meadowrun-0.2.0-py_0.tar.bz2`
REM If you're running this for the first time, you'll need to run `anaconda login`
REM before you run `anaconda upload`
