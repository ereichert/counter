from package import *
from build import *
from release import *
from fabric.network import ssh
from fabric.api import env, put, run, sudo, task
from fabric.decorators import runs_once

ssh.util.log_to_file("paramiko.log", 10)
env.use_ssh_config = True

TEMPLATES_DIR = os.path.join(DEPLOYMENT_WORKING_DIR, "templates")
print("TEMPLATES_DIR = {}".format(TEMPLATES_DIR))
WORKSPACE_DIR = os.path.join(DEPLOYMENT_WORKING_DIR, "templates/workspace/")
print("WORKSPACE_DIR = {}".format(WORKSPACE_DIR))
VALID_MODES = ["full", "dryrun"]


@runs_once
@task
def release_final():
    run_release(RELEASE_TYPE_FINAL)


@runs_once
@task
def release_snapshot():
    run_release(RELEASE_TYPE_SNAPSHOT)


@runs_once
@task
def release_test_final():
    release_context = ReleaseContext(
        PROJECT_ROOT,
        RELEASE_TYPE_TEST_FINAL,
        "{}/Cargo.toml".format(PROJECT_ROOT),
        "{}/src/version.txt".format(PROJECT_ROOT),
        "{}/README.md".format(PROJECT_ROOT),
        True,
        True,
        True
    )
    release(release_context)
    bump_version(release_context)


def run_release(release_type):
    release_context = ReleaseContext(
        PROJECT_ROOT,
        release_type,
        "{}/Cargo.toml".format(PROJECT_ROOT),
        "{}/src/version.txt".format(PROJECT_ROOT),
        "{}/README.md".format(PROJECT_ROOT),
        False,
        False,
        True
    )
    release(release_context)
    rpm_path = package(release_context)
    env.host_string = YUM_REPO_HOST
    print("Publishing to {}.".format(env.host_string))
    publish_rpm(rpm_path)
    bump_version(release_context)
    print "Pushing release to origin."
    release_context.push_to_origin()


@runs_once
@task
def publish_rpm(local_rpm_path):
    if os.path.isfile(local_rpm_path):
        put(local_rpm_path, YUM_REPO_PATH, use_sudo=True)
        sudo("chown root:root -R {} ".format(YUM_REPO_PATH))
    else:
        fabric.utils.abort("The RPM does not exist at {}.".format(local_rpm_path))


@task
def deploy(mode="dryrun"):
    bad_hosts = check_hosts(env.hosts)
    if bad_hosts:
        fabric.utils.abort("You specified the following invalid hosts: {}".format(bad_hosts))

    if not is_valid_mode(mode):
        fabric.utils.abort("You must specify a valid mode: mode={}".format(VALID_MODES))

    print("Running in mode {}.".format(mode))
    result = sudo("yum -q list installed counter", warn_only=True)
    yum_cmd = "yum update -y counter" if result.return_code == 0 else "yum install -y counter"
    if mode != "dryrun":
        result = sudo(yum_cmd, warn_only=True)
        if result.return_code == 0:
            sudo("service counter restart")
            sudo("chkconfig counter on")
            deploy_site_logos()
            deploy_consul_service_definition()
        else:
            print("!!! FAILED TO RUN {}. !!!".format(yum_cmd))


@runs_once
def check_hosts(hosts):
    print
    print "Checking hosts {}.".format(hosts)
    original_host_string = env.host_string
    bad_hosts = []
    for host in hosts:
        env.host_string = host
        try:
            run("hostname")
        except Exception as e:
            bad_hosts.append(host)

    fabric.network.disconnect_all()
    env.host_string = original_host_string
    return bad_hosts


def template_writer(template_name, rendered):
    if not os.path.exists(WORKSPACE_DIR):
        os.makedirs(WORKSPACE_DIR)
    rendered_file = os.path.join(WORKSPACE_DIR, template_name)
    print("Template rendered to {rf}".format(rf=rendered_file))
    with open(rendered_file, "w") as text_file:
        text_file.write(rendered)


def is_valid_mode(mode):
    return mode in VALID_MODES


@task
def deploy_consul_service_definition():
    filename = "counter.json"
    template = Template(filename=os.path.join(TEMPLATES_DIR, "consul_service_definition.json.template"))
    rendered = template.render(
        hostname=env.host
    )
    template_writer(filename, rendered)
    consul_file = os.path.os.path.join(WORKSPACE_DIR, filename)
    print "Deploying {}.".format(consul_file)
    put(consul_file, "/etc/consul/", use_sudo=True)
    sudo("service consul restart")


@task
def deploy_site_logos():
    site_name = env.host.split('.')[1]
    logo_path = os.path.join(LOGO_ROOT, site_name)
    if os.path.isdir(logo_path):
        print "Logo directory found for site {}, deploying".format(site_name)
        put(os.path.join(logo_path, '*'), REMOTE_LOGO, use_sudo=True)
    else:
        print "No logos found for site {}".format(site_name)
