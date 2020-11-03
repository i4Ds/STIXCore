Developers Guide
================

Fork and Clone Repository
-------------------------
Working of your own forked version of the repository is the preferred approach. To fork the
repository visit the repository page at <https://github.com/i4Ds/STIXCore> (make sure you are logged
into github) and click on the fork button at the to right of the page

Clone your forked version of the

.. code:: bash

    git clone https://github.com/<username>/STIXCore.git

It is also advisable to configure the upstream remote at this point

.. code:: bash

    git remote add upstream https://github.com/i4Ds/STIXCore


Isolated Environment
--------------------
It is highly recommended to work in an isolated python environment there are a number of tools
available to help mange and create isolated environment such as

* `Anaconda <https://anaconda.org>`__
* `Pyenv <https://github.com/pyenv/pyenv>`__
* Python 3.6+ inbuilt venv.

For this documentation we will proceed using Python's venv but the step would be similar in other
tools.

First verify the python version installed by running `python -V` or possibly `python3 -V` depending
on your system it should be greater then 3.6

.. code:: bash

    python3 -m venv /path/to/new/virtual/environment

Working on code
---------------
It's import to always be working from the most recent version of the so before working on any code
start by getting the latest changes and then creating a branch for you new code.

.. code:: bash

    git checkout master
    git pull upstream master
    git checkout -b <branch-name>

Branch names should ideally be short and descriptive e.g. 'feature-xmlparseing`, 'bugfix-ql-fits`,
'docs-devguide` and perferably seperated by dashes `-` rather than underscores `_`.


Testing
-------


Documentation
-------------
