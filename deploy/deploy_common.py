import os

DEPLOYMENT_WORKING_DIR = os.getcwd()
print("DEPLOYMENT_WORKING_DIR = {}".format(DEPLOYMENT_WORKING_DIR))

PROJECT_ROOT = os.path.dirname(DEPLOYMENT_WORKING_DIR)
print("PROJECT_ROOT = {}".format(PROJECT_ROOT))

ROOT_ACCT = "root"
print("ROOT_ACCT = {}".format(ROOT_ACCT))

YUM_REPO_HOST = "puppet.prod.tl.com"
print("YUM_REPO_HOST = {}".format(YUM_REPO_HOST))

YUM_REPO_PATH = "/opt/yumrepo"
print("YUM_REPO_PATH = {}".format(YUM_REPO_PATH))

REMOTE_SERVICE_ACCT = ""
print("REMOTE_SERVICE_ACCT = {}".format(REMOTE_SERVICE_ACCT))


def result_handler(result, message, return_code=0):
    if result.failed or result.return_code != return_code:
        print result
        raise Exception()
    else:
        print message
