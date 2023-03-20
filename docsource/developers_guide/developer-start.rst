General
*******

This section explains what you need to do as a Jobmon coder, plus gives a brief introduction to the major
technologies. More detailed notes on technologies and design are in the Architecture & Deisgn section.

******************************
Working on the Jobmon Codebase
******************************

The standard workflow for contributing to Jobmon is:

#. Create a feature branch from the branch for the next release
#. Make your changes on that branch
#. Add or modify the unit tests for your new code
#. Run the unit tests and fix them
#. Lint and type check the code
#. Create a pull request
   #. Ensure that the automatic builds pass
#. Gain approval from at least 2 members of the Scicomp team

Nox and pytest
^^^^^^^^^^^^^^

The test suite uses nox to manage virtual testing environments and install the necessary dependencies, and pytest to
define common fixtures such as a temporary database and web service.
For more details on the unit test architecture, please refer to the Developer Testing section.

Running unit tests
******************

To run the Jobmon test suite, navigate to the top-level folder in this repository and run ``nox -s tests -- tests/``.

To reduce the runtime of the tests, you can optionally suffix the above command with ``-n=<number_of_processes>`` to
enable testing in parallel. You can also use ``nox -r ...`` to re-use an existing virtual environment.

End-to-end tests
****************
As part of the continuous integration pipeline, IHME-TAD repository automatically deploys a complete installation
of Jobmon and runs post-deployment tests.
This catches any mismatches between jobmon-core and the jobmon-slurm plugin.


Linting and Typechecking
************************

To run linting and type checking, run ``nox -s lint`` and ``nox -s typecheck`` respectively.

The linting check uses flake8 to check that the code conforms to pep8 formatting standards, with exceptions as defined
in setup.cfg.
Type checking uses mypy ensures that our code has the correct type hints and usages conforming to
`PEP484 <https://www.python.org/dev/peps/pep-0484/>`_.
Type hints catch errors earlier and makes the large codebase much easier to read.

Sometimes the linting check will fail with a message indicating that "Black would make changes". Black is an
autoformatting tool that ensures code conformity. To address this error you can run ``nox -s black``.

Code must pass linting and typechecking before it can be merged.

Pull Requests
*************

When the above unit tests, formatting checks, and typing checks all pass, you can submit a pull request on Stash. Add
all members of the Scicomp team to the created pull request.

Creating a pull request should start an automatic build, which runs the above mentioned tests and checks on a
the scicomp Jenkins server
`<https://jenkins.scicomp.ihme.washington.edu>`_

If all the tests pass, you will see a green check mark on the builds page in your PR.

