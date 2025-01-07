---
name: Bug report
about: Create a report to help us improve
title: "[BUG]"
labels: 'bug'
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Command executed:
* mydumper with all the parameters
* myloader with all the parameters

What mydumper and myloader version has been used?

**Expected behavior**
A clear and concise description of what you expected to happen.

**Log**
If applicable, add the --verbose 4 / -v 4 and --logfile / -L <filename> to the execution of the command and upload the file.
Take into account that you can also use --masquerade-filename to do not expose database and table names.

**Backup**
If applicable, add the list of content of the database path, an `ls -l` will be enough.
Take into account that you can also use --masquerade-filename to do not expose the filenames.

**How to repeat**
If applicable, add the minimal table structure and data that we need to reproduce the issue.
Or upload a [core dump](https://github.com/mydumper/mydumper/wiki/Support#core-dump) for cases where mydumper or myloader is crashing.

**Environment (please complete the following information):**
 - OS version: [e.g. CentOS 7.9, Ubuntu 20.04]
 - MyDumper version: [ mydumper --version ]

**Additional context**
Add any other context about the problem here.
