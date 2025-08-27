Developers Guide
================

The instructions in this following section are based upon resources:

* `Astropy Dev Workflow <https://docs.astropy.org/en/latest/development/workflow/development_workflow.html>`_
* `Astropy Dev environment <https://docs.astropy.org/en/latest/development/workflow/get_devel_version.html#get-devel>`_
* `Astropy Pull Request Example <https://docs.astropy.org/en/latest/development/workflow/git_edit_workflow_examples.html#astropy-fix-example>`_
* `Sunpy Newcomers' Guide <https://docs.sunpy.org/en/latest/dev_guide/newcomers.html>`_

Fork and Clone Repository
-------------------------
Working of your own forked version of the repository is the preferred approach. To fork the
repository visit the repository page at https://github.com/i4Ds/STIXCore (make sure you are logged
into github) and click on the fork button at the to right of the page.

Enable git Large File Support (lsf) for your user account if not done already.

.. code:: bash

    git lfs install

Clone your forked version of the

.. code:: bash

    git clone https://github.com/<username>/STIXCore.git

It is also advisable to configure the upstream remote at this point

.. code:: bash

    git remote add upstream https://github.com/i4Ds/STIXCore


Isolated Environment
--------------------
It is highly recommended to work in an isolated python environment there are a number of tools
available to help manage and create isolated environment such as

* `Anaconda <https://anaconda.org>`__
* `Pyenv <https://github.com/pyenv/pyenv>`__
* Python 3.6+ inbuilt venv.

For this documentation we will proceed using Python's venv but the step would be similar in other
tools.

First verify the python version installed by running `'python -V'` or possibly `'python3 -V'` depending
on your system it should be greater then 3.6. Next create a new virtual environment in a directory
outside the git repo and activate.

.. code:: bash

    python3 -m venv /path/to/new/virtual/environment
    source /path/to/new/virtual/environment/bin/activate
    #note the prompt change

The next step is to install the required dependencies (ensure you are working from inside you virtual
environment which can be verified by comparing the path returned from `'python -m pip -V'` to the path
used in the above steps)

.. code:: bash

    python -m pip install -e .
    # to install all development dependencies documentation, testing etc use
    python -m pip install -e .[dev]


Working on code
---------------
It's import to always be working from the most recent version of the so before working on any code
start by getting the latest changes and then creating a branch for you new code.

.. code:: bash

    git checkout master
    git pull upstream master
    git checkout -b <branch-name>

Branch names should ideally be short and descriptive e.g. 'feature-xmlparseing', 'bugfix-ql-fits',
'docs-devguide' and preferably separated by dashes '-' rather than underscores '_'.

Once you are happy with your changes push the changes to github

.. code:: bash

    git add <list of modified or changed files>
    git commit
    git push origin <branch-name>

and open a pull request (PR).

Note a series of checks will be automatically run on code once a PR is created it is recommended
that you locally test the code as outlined below. Additionally it is  recommended that you install
and configure `pre-commit <https://pre-commit.com>`_ which runs various style and code quality
checks before commit.

.. code:: bash

    python -m pip install pre-commit
    pre-commit install


Testing
-------
Testing is built on the `PyTest <https://docs.pytest.org/en/stable/>`_ and there are a number of
ways to run the tests. During development it is often beneficial to run a subset of
test relevant to the current code this can be accomplished by running one of the commands below.

.. code:: bash

    pytest stixcore/path/to/test_file.py:test_one        # run a specific test function
    pytest stixcore/path/to/test_file.py                 # run a specific test file
    pytest stixcore/module                               # run all test for a modules
    pytst                                                # run all tests


Additionally `tox <https://tox.readthedocs.io/en/latest/>`_ is use to create and run tests in
reproducible environments. To see a list of tox environment use `'tox -l'` to run a specific
environment run `'tox -e <envname>'` or to run all simply run `'tox'`.

.. note::

    This is the same process that is run on the CI


Documentation
-------------
Documentation is built using `Sphinx <https://www.sphinx-doc.org/en/master/>`_ similarly to the
tests above this can be run manually or through tox. To run manually cd to the docs directory and
run `'make html'` to run via tox `'tox -e build_docs'`. There is a known dependency on Graphviz.
If you have any problems (on Windows) follow `this <https://bobswift.atlassian.net/wiki/spaces/GVIZ/pages/20971549/How+to+install+Graphviz+software>`_ instructions.

End to End Testing
------------------

Changing the code base might result in a change of the generated FITS data products the processing pipelines are generating. This might happen on purpose or unintentionally while enhancing data definitions (structural changes in the FITS extensions and header keyword) but also in the data itself due to changed number crunching methods. To avoid unnoticed changes in the generated fits files there is the end 2 end testing hook in place. If many of the number crunching methods are covered by unit test this additional test is to ensure the data integrity. If a change of a FITS product was on purpose a new version for that product has to be released, reprocessed and delivered to SOAR

This additional test step can be triggered locally but is also integrated into the CI get actions. In order to merge new PRs the test have to pass or the failure manually approved.

Provide test data to compare to
*******************************
A predefined set of FITS products together with the original TM data that was used to create them are public available on the processing server as zip file: https://pub099.cs.technik.fhnw.ch/data/end2end/data/head.zip . This TM data is used to generate new FITS products with th elates code base and afterwards compared for completeness and identical data.

Running the tests
*****************

The end to end tests are defines as normal unit tests here stixcore/processing/tests/test_end2end.py but marked with @pytest.mark.end2end. In the CI runs on two separate test runs one for the end to end test and one for all others unit tests.

run it with `pytest -v  -m end2end`

Manually approve failed end to end tests
****************************************

Before a PR can be merged a set of test have to pass in the CI including the end to end testing. If you have changed the code base that way that your generated test fits product are not identical with the original test files you will be noted by a failed test result.

If your changes where intended and your are happy with the reported differences of the original and current test fits products a repo admin can merge the PR by bypassing the test in the GitHub UI. If you are not happy that changes where happen att all rework your code until you can explain the reported matching errors.

Update the original test data
*****************************

On each merge to the git master branch a web hook (https://pub099.cs.technik.fhnw.ch/end2end/rebuild_hook.cgi - credentials stored as git secrets) is triggered to regenerate the original test data and TM source data and the https://pub099.cs.technik.fhnw.ch/data/end2end/data/head.zip gets replaced with the latest data. For that regenerating of the data a dedicated STIXCore environment is running on pub099 (/data/stix/end2en). That STIXCore environment always pulls the latest code updates from the master branch and reprocess the data. That way each new PR has to generated identical data as the last approved merged PR or the needs manual approval for the detected changes.
