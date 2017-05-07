## deployment
Deployment is handled by the python tool http://www.fabfile.org/.

You will create a virtual environment using virtualenv http://docs.python-guide.org/en/latest/dev/virtualenvs/.

You will need the Development Tools, python-devel, and rpm-build packages installed on the CentOS instance.

To build an RPM the build process must be run on a Linux host.  We use CentOS.

If you are not running a CentOS host natively you will need a virtual machine with CentOS installed.

Once you have a base CentOS installation you must prepare it to use the deployment tools.  
The following link will walk you through setting up your Linux host.
 
https://www.digitalocean.com/community/tutorials/how-to-set-up-python-2-7-6-and-3-3-3-on-centos-6-4

Once you have the deployment tools installed, deployment is easy.

From the Counter source code directory.

```
cd deploy
virtualenv virt_env
source virt_env/bin/activate (you can deactivate using the deactivate command)
```

Use the requirements.txt file located in the ../deploy directory to install all needed dependencies.

```
pip install -r requirements.txt
```

Ensure git is configured with your information.

```
git config --global --get-regexp 'user.*'
```

Once your virtual environment is installed run fabric tasks as follows.

```
fab <task>:<task_params>
```

deploy task options:

mode - [ full | dryrun ] - default:dryrun

full = do the deployment

dryrun = do everything with the exception of the push to the server

### Release

In general you should use the release scripts to run the fab tasks that manage a release.  
To run a release you must be on the develop branch and it must be clean.

```
fab release_snapshot xor fab release_final
```

However, to run a release manually, for testing script updates for example, you can use the following command format.

```
fab release:release_type=<snapshot ^ final ^ testfinal>,<disable_checks=True>,<dry_run=False>
```

Most testing should be done using testfinal.
