import shutil
import fabric
from fabric.api import local, task
from fabric.decorators import runs_once
from mako.template import Template
from deploy_common import *


@runs_once
@task
def package(release_context):
    package_name = release_context.package_name()
    print("Packaging {} for deployment.".format(package_name))

    release_bin_path = os.path.join(PROJECT_ROOT, "target/release/{}".format(package_name))
    if os.path.isfile(release_bin_path):
        print("release_bin_path = {}".format(release_bin_path))
    else:
        fabric.utils.abort("The release binary does not exist at {}.".format(release_bin_path))

    version_path = os.path.join(PROJECT_ROOT, "src/version.txt")
    with open(version_path, 'r') as version_file:
        expected_version = version_file.read().strip()

    version_cmd = "{} --version".format(release_bin_path)
    version_cmd_result = local(version_cmd, capture=True).replace("{} ".format(release_context.package_name()), "").strip()

    if expected_version != version_cmd_result:
        fabric.utils.abort(
            "The version of the binary,\n\n{reported_version}\n\n, "
            "is not the same as the version specified in {ver_path},\n\n{expected_version}\n\n. "
            "This is usually the result of failing to build the release build of {pkg_name} before trying package it."
            .format(
                reported_version=version_cmd_result,
                ver_path=version_path,
                expected_version=expected_version,
                pkg_name=package_name
            )
        )

    else:
        print("Validated version {}".format(version_cmd_result))

    deployment_version = version_cmd_result.split('\n', 1)[0].replace("-", ".")

    rpmbuild_root = os.path.join(DEPLOYMENT_WORKING_DIR, "rpmbuild")
    print("rpmbuild_root = {}".format(rpmbuild_root))

    init_rpmbuild_root(rpmbuild_root)

    build_root = os.path.join(os.path.join(rpmbuild_root, "BUILDROOT"),
                              "{}-{}-1.el6.x86_64".format(package_name, deployment_version))
    print("build_root = {}".format(build_root))
    prod_arts = enumerate_production_artifacts(PROJECT_ROOT, package_name)
    prod_arts.extend(assets_artifacts(PROJECT_ROOT, package_name, "assets"))
    create_production_artifacts(build_root, prod_arts)
    artifact_list = generate_artifact_list(prod_arts)
    print("Writing the following artifact list to the RPM spec file.\n{}".format(artifact_list))
    write_spec_file(
        artifact_list,
        deployment_version,
        os.path.join(DEPLOYMENT_WORKING_DIR, "templates/{}.spec".format(package_name)),
        os.path.join(rpmbuild_root, "SPECS/{}.spec".format(package_name)),
    )

    build_cmd = "rpmbuild --define \"_topdir {pbd}\" -bb rpmbuild/SPECS/{pkg_name}.spec".format(
        pbd=rpmbuild_root,
        pkg_name=package_name
    )
    local(build_cmd)
    return os.path.join(DEPLOYMENT_WORKING_DIR, "rpmbuild/RPMS/x86_64/{}".format(
        "{}-{}-1.el6.x86_64.rpm".format(package_name, deployment_version)))


def init_rpmbuild_root(root_dir):
    shutil.rmtree(root_dir, ignore_errors=True)
    rpmbuild_dirs = ["BUILD", "BUILDROOT", "RPMS", "SOURCES", "SPECS", "SRPMS", "TMP"]
    print("Creating rpmbuild directory structure.")
    for folder_name in rpmbuild_dirs:
        folder_path = os.path.join(root_dir, folder_name)
        print("Creating directory {}.".format(folder_path))
        os.makedirs(folder_path)


def write_spec_file(artifact_list, release_version, spec_template_path, spec_dst):
    template = Template(filename=spec_template_path)
    rendered = template.render(
        artifact_list=artifact_list,
        version=release_version
    )
    with open(spec_dst, "w") as spec_file:
        spec_file.write(rendered)


def enumerate_production_artifacts(project_root, package_name):
    prod_dir_names = []
    prod_arts = [
        BuildArtifact(
            dst="/usr/bin/{}".format(package_name),
            src=os.path.join(project_root, "target/release/{}".format(package_name)),
            is_dir=False
        )
    ]
    for folder_name in prod_dir_names:
        prod_arts.append(BuildArtifact(dst=os.path.join("/opt/trafficland/{}".format(package_name), folder_name)))
    return prod_arts


def assets_artifacts(project_root, package_name, assets_dir):
    asset_arts = []
    for root, dirs, files in os.walk(os.path.join(project_root, assets_dir)):
        base = os.path.relpath(root, project_root)
        for dirname in dirs:
            asset_arts.append(
                BuildArtifact(
                    dst=os.path.join("/opt/trafficland/{}/".format(package_name), base, dirname),
                    is_dir=True,
                )
            )
        for filename in files:
            asset_arts.append(
                BuildArtifact(
                    dst=os.path.join("/opt/trafficland/{}/".format(package_name), base, filename),
                    src=os.path.join(root, filename),
                    is_dir=False
                )
            )
    return asset_arts


def create_production_artifacts(build_root, prod_arts):
    print("Creating the production artifacts.")
    for artifact in prod_arts:
        build_path = os.path.join(build_root, artifact.dst.strip("/"))
        build_dir = build_path if artifact.is_dir else os.path.dirname(build_path)
        if not os.path.exists(build_dir):
            print("Creating directory {bd}.".format(bd=build_dir))
            os.makedirs(build_dir)
        if not artifact.is_dir:
            print("Copying {src} to {bp}".format(src=artifact.src, bp=build_path))
            shutil.copy(artifact.src, build_path)


def generate_artifact_list(prod_arts):
    art_set = set()
    for artifact in prod_arts:
        add_paths(art_set, artifact.dst, artifact.is_dir, artifact.is_config)
    sorted_art_set = sorted(art_set, key=count_slashes)
    artifact_list = ""
    for artifact in sorted_art_set:
        if artifact.is_dir:
            artifact_list += "%dir "
        elif artifact.is_config:
            artifact_list += "%config "
        artifact_list += "%attr(0755,{owner},{owner}) {path}\n".format(owner=artifact.owner, path=artifact.dst)
    return artifact_list


# The first time through the is_dir parameter is determined by the upstream caller and could be False for a file.
# Subsequent recursive calls should set is_dir to false because it's impossible for a second call to be anything but a
# directory.
def add_paths(acc, path, is_dir, is_config):
    directory_blacklist = ["/", "/opt", "/etc", "/etc/init.d"]
    if path not in directory_blacklist:
        acc.add(
            BuildArtifact(
                dst=path,
                owner=REMOTE_SERVICE_ACCT if path_owned_by_project(path) else ROOT_ACCT,
                is_dir=is_dir,
                is_config=is_config,
            )
        )
        split = os.path.split(path)
        add_paths(acc, split[0], True, False)


def count_slashes(artifact):
    return artifact.dst.count("/")


def path_owned_by_project(path):
    return path.startswith("/opt/trafficland")


class BuildArtifact:
    src = ""
    dst = ""
    owner = REMOTE_SERVICE_ACCT
    is_dir = True
    is_config = False

    def __init__(self, dst, src="", owner=REMOTE_SERVICE_ACCT, is_dir=True, is_config=False):
        assert not(is_dir and is_config), "A build artifact cannot be both a directory and a config"
        self.src = src
        self.dst = dst
        self.owner = owner
        self.is_dir = is_dir
        self.is_config = is_config

    def __repr__(self):
        return "BuildArtifact(dst = {dst}, src = {src}, owner = {o}, is_dir = {dir}), is_config = {config}" \
            .format(
            dst=self.dst,
            src=self.src,
            o=self.owner,
            dir=self.is_dir,
            config=self.is_config
        )

    # __hash__ and __eq__ are very, very naive but they should be fine for what we're trying to do with them.
    def __hash__(self):
        return self.dst.__hash__()

    def __eq__(self, other):
        return self.dst == other.dst
