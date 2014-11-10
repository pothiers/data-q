data-queue
==========

Process records of incoming data files, apply various actions on pop,
be resilient.

The src/dataq is intended to be put in /etc/rc.d/init.d (on Scientific
Linux OS, similar place on other OS).   Starts and stops the required daemons

Detailed documention is in the "docs" directory.

After the daemons are started ("sudo service dataq start"), 
you can run smoke tests:
  tests/smoke.sh

If all goes well, you will get the message:
  ALL smoke tests PASSED (README-smoke-results.txt created)

