# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (C) 2012-2020 British Crown (Met Office) & Contributors.
#
# This file is part of Rose, a framework for meteorological suites.
#
# Rose is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Rose is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Rose. If not, see <http://www.gnu.org/licenses/>.
# -----------------------------------------------------------------------------
"""rose stem [options] [path]

Install a suitable suite with a specified set of source tree(s).

To run a rose-stem suite use "cylc play".

Default values of some of these settings are suite-dependent, specified
in the `rose-suite.conf` file.

Examples

    rose stem --group=developer
    rose stem --source=/path/to/source --source=/other/source --group=mygroup
    rose stem --source=foo=/path/to/source --source=bar=fcm:bar_tr@head

Rose Stem Plugin

    There is a new "rose stem" plugin which runs automatically whenever any
    Cylc command is run against a "rose stem" source repository.

    This is a new experimental feature that allows you to use all Cylc commands
    with Rose Stem test suites, including "cylc config", "cylc graph" and
    "cylc reload".

    To use the new plugin, call Cylc commands as you would for a regular Cylc
    workflow, providing any tasks/groups using the "-z" option e.g:

    # old rose stem command
    $ rose stem --group=developer,canary --source=mysource=$PWD --source=foo
    $ cylc play <workflow-id>

    # new rose stem plugin
    $ cylc vip -z group=developer,canary ./rose-stem --source=foo

Jinja2 Variables

    Note that `<project>` refers to the FCM keyword name of the repository in
    upper case.

    HOST_SOURCE_<project>
        The complete list of source trees for a given project. Working copies
        in this list have their hostname prefixed, e.g. `host:/path/wc`.
    HOST_SOURCE_<project>_BASE
        The base of the project specified on the command line. This is
        intended to specify the location of `fcm-make` config files. Working
        copies in this list have their hostname prefixed.
    RUN_NAMES
        A list of groups to run in the rose-stem suite.
    SOURCE_<project>
        The complete list of source trees for a given project. Unlike the
        `HOST_` variable of similar name, paths to working copies do NOT
        include the host name.
    SOURCE_<project>_BASE
        The base of the project specified on the command line. This is
        intended to specify the location of `fcm-make` config files. Unlike
        the `HOST_` variable of similar name, paths to working copies do NOT
        include the host name.
    SOURCE_<project>_REV
        The revision of the project specified on the command line. This
        is intended to specify the revision of `fcm-make` config files.
"""

from copy import deepcopy
from contextlib import suppress
from optparse import OptionGroup
import os
from pathlib import Path
import re
import sys
from typing import Tuple

from ansimarkup import parse as cparse

from cylc.flow import LOG
from cylc.flow.exceptions import CylcError, InputError
from cylc.flow.scripts.install import get_option_parser
from cylc.flow.scripts.install import install as cylc_install
import metomi.rose.config
from metomi.rose.fs_util import FileSystemUtil
from metomi.rose.host_select import HostSelector
from metomi.rose.popen import RosePopener
from metomi.rose.reporter import Event, Reporter
from metomi.rose.resource import ResourceLocator

from cylc.rose.entry_points import (
    export_environment,
    load_rose_config,
)
from cylc.rose.utilities import (
    id_templating_section,
    process_config,
)

EXC_EXIT = cparse('<red><bold>{name}: </bold>{exc}</red>')
DEFAULT_TEST_DIR = 'rose-stem'
ROSE_STEM_VERSION = 1


class ConfigVariableSetEvent(Event):

    """Event to report a particular variable has been set."""

    LEVEL = Event.V

    def __repr__(self):
        return "Variable %s set to %s" % (self.args[0], self.args[1])

    __str__ = __repr__


class NameSetEvent(Event):

    """Event to report a name for the suite being set.

    Simple parser of output expected to be in the format:
    Key: Value.
    """

    LEVEL = Event.V

    def __repr__(self):
        return "Workflow is named %s" % (self.args[0])

    __str__ = __repr__


class ProjectNotFoundException(CylcError):

    """Exception class when unable to determine project a source belongs to."""

    def __init__(self, source, error=None):
        Exception.__init__(self, source, error)
        self.source = source
        self.error = error

    def __repr__(self):
        if self.error is not None:
            return "Cannot ascertain project for source tree %s:\n%s" % (
                self.source, self.error)
        else:
            return "Cannot ascertain project for source tree %s" % (
                self.source)

    __str__ = __repr__


class RoseStemVersionException(CylcError):

    """Exception class when running the wrong rose-stem version."""

    def __init__(self, version):

        Exception.__init__(self, version)
        if version is None:
            self.suite_version = (
                "does not have ROSE_STEM_VERSION set in the "
                "rose-suite.conf"
            )
        else:
            self.suite_version = "at version %s" % (version)

    def __repr__(self):
        return "Running rose-stem version %s but suite is %s" % (
            ROSE_STEM_VERSION, self.suite_version)

    __str__ = __repr__


class RoseSuiteConfNotFoundException(CylcError):

    """Exception class when unable to find rose-suite.conf."""

    def __init__(self, location):
        Exception.__init__(self, location)
        self.location = location

    def __repr__(self):
        if os.path.isdir(self.location):
            return "\nCannot find a suite to run in directory %s" % (
                self.location)
        else:
            return "\nSuite directory %s is not a valid directory" % (
                self.location)

    __str__ = __repr__


class SourceTreeAddedAsBranchEvent(Event):

    """Event to report a source tree has been added as a branch."""

    LEVEL = Event.DEFAULT

    def __repr__(self):
        return "Source tree %s added as branch" % (self.args[0])

    __str__ = __repr__


class SourceTreeAddedAsTrunkEvent(Event):

    """Event to report a source tree has been added as a trunk."""

    LEVEL = Event.DEFAULT

    def __repr__(self):
        return "Source tree %s added as trunk" % (self.args[0])

    __str__ = __repr__


class SuiteSelectionEvent(Event):

    """Event to report a source tree for config files."""

    LEVEL = Event.DEFAULT

    def __repr__(self):
        return "Will install suite from %s" % (self.args[0])

    __str__ = __repr__


class StemRunner:

    """Set up options for running a STEM job through Rose."""

    def __init__(self, opts, reporter=None, popen=None, fs_util=None):
        self.opts = opts

        if reporter is None:
            self.reporter = Reporter(opts.verbosity - opts.quietness)
        else:
            self.reporter = reporter

        if popen is None:
            self.popen = RosePopener(event_handler=self.reporter)
        else:
            self.popen = popen

        if fs_util is None:
            self.fs_util = FileSystemUtil(event_handler=self.reporter)
        else:
            self.fs_util = fs_util

        self.host_selector = HostSelector(event_handler=self.reporter,
                                          popen=self.popen)
        self.template_section = '[template variables]'

        # results get stored here
        self.env = {}
        self.template_variables = {}

    def _add_template_var(self, var, val):
        """Add a define option passed to the SuiteRunner.

        Args:
            var: Name of variable to set
            val: Value of variable to set
        """
        self.template_variables[var] = val
        self.reporter(ConfigVariableSetEvent(var, val))

    def _get_fcm_loc_layout_info(self, src_tree):
        """Given a source tree return the following from 'fcm loc-layout':
           * url
           * sub_tree
           * peg_rev
           * root
           * project
        """

        ret_code, output, stderr = self.popen.run(
            'fcm', 'loc-layout', src_tree)
        if ret_code != 0:
            raise ProjectNotFoundException(src_tree, stderr)

        ret = {}
        for line in output.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)

            if key and value:
                ret[key] = value.strip()

        return ret

    def _get_project_from_url(self, source_dict):
        """Run 'fcm keyword-print' to work out the project name."""
        repo = source_dict['root']
        if source_dict['project']:
            repo += '/' + source_dict['project']

        kpoutput = self.popen.run('fcm', 'kp', source_dict['url'])[1]
        project = None
        for line in kpoutput.splitlines():
            if line.rstrip().endswith(repo):
                kpresult = re.search(r'^location{primary}\[(.*)\]', line)
                if kpresult:
                    project = kpresult.group(1)
                    break
        return project

    @staticmethod
    def _deduce_mirror(source_dict, project):
        """Deduce the mirror location of this source tree."""

        # Root location for project
        proj_root = source_dict['root'] + '/' + source_dict['project']

        # Swap project to mirror
        project = re.sub(r'\.x$', r'.xm', project)
        mirror_repo = "fcm:" + project

        # Generate mirror location
        mirror = re.sub(proj_root, mirror_repo, source_dict['url'])

        # Remove any sub-tree
        mirror = re.sub(source_dict['sub_tree'], r'', mirror)
        mirror = re.sub(r'/@', r'@', mirror)

        # Add forwards slash after .xm if missing
        if '.xm/' not in mirror:
            mirror = re.sub(r'\.xm', r'.xm/', mirror)
        return mirror

    def _ascertain_project(self, item, warn_source=True):
        """Set the project name and top-level from 'fcm loc-layout'.
        Returns:
        - project name
        - top-level location of the source tree with revision number
        - top-level location of the source tree without revision number
        - revision number
        """
        project = None
        with suppress(ValueError):
            project, item = item.split("=", 1)

        if re.search(r'^\.', item):
            item = os.path.abspath(os.path.join(os.getcwd(), item))

        if project:
            if warn_source:
                LOG.warning(f"Forcing project for '{item}' to be '{project}'")
            return project, item, item, '', ''

        source_dict = self._get_fcm_loc_layout_info(item)
        project = self._get_project_from_url(source_dict)
        if not project:
            raise ProjectNotFoundException(item)

        mirror = self._deduce_mirror(source_dict, project)

        if 'peg_rev' in source_dict and '@' in item:
            revision = '@' + source_dict['peg_rev']
            base = re.sub(r'@.*', r'', item)
        else:
            revision = ''
            base = item

        # Remove subtree from base and item
        if 'sub_tree' in source_dict:
            item = re.sub(
                r'(.*)%s/?$' % (source_dict['sub_tree']), r'\1', item, count=1)
            base = re.sub(
                r'(.*)%s/?$' % (source_dict['sub_tree']), r'\1', base, count=1)

        # Remove trailing forwards-slash
        item = re.sub(r'/$', r'', item)
        base = re.sub(r'/$', r'', base)

        # Remove anything after a point
        project = re.sub(r'\..*', r'', project)
        return project, item, base, revision, mirror

    def _generate_name(self):
        """Generate a suite name from the name of the first source tree."""
        try:
            basedir = self._ascertain_project(os.getcwd())[1]
        except ProjectNotFoundException:
            if self.opts.workflow_conf_dir:
                basedir = os.path.abspath(self.opts.workflow_conf_dir)
            else:
                basedir = os.path.abspath(os.getcwd())

        name = os.path.basename(basedir)
        self.reporter(NameSetEvent(name))
        return name

    def _this_suite(self):
        """Find the location of the suite in the first source tree."""

        # Get base of first source
        basedir = ''
        if self.opts.stem_sources:
            basedir = self.opts.stem_sources[0]
        else:
            basedir = self._ascertain_project(os.getcwd())[1]

        suitedir = os.path.join(basedir, DEFAULT_TEST_DIR)
        suitefile = os.path.join(suitedir, "rose-suite.conf")

        if not os.path.isfile(suitefile):
            raise RoseSuiteConfNotFoundException(suitedir)

        self.opts.suite = suitedir

        self._check_suite_version(suitefile)

        return suitedir

    def _read_auto_opts(self):
        """Read the site rose.conf file."""
        return ResourceLocator.default().get_conf().get_value(
            ["rose-stem", "automatic-options"])

    def _check_suite_version(self, fname):
        """Check the suite is compatible with this version of rose-stem."""
        if not os.path.isfile(fname):
            raise RoseSuiteConfNotFoundException(os.path.dirname(fname))
        config = metomi.rose.config.load(fname)
        suite_rose_stem_version = config.get(['ROSE_STEM_VERSION'])
        if suite_rose_stem_version:
            suite_rose_stem_version = int(suite_rose_stem_version.value)
        else:
            suite_rose_stem_version = None
        if not suite_rose_stem_version == ROSE_STEM_VERSION:
            raise RoseStemVersionException(suite_rose_stem_version)

    def _prepend_localhost(self, url):
        """Prepend the local hostname to urls which do not point to repository
        locations."""
        if ':' not in url or url.split(':', 1)[0] not in ['svn', 'fcm', 'http',
                                                          'https', 'svn+ssh']:
            url = self.host_selector.get_local_host() + ':' + url
        return url

    def _parse_auto_opts(self):
        """Load the site config file and return any automatic-options.

        Parse options in the form of a space separated list of key=value
        pairs.
        """
        auto_opts = self._read_auto_opts()
        if auto_opts:
            automatic_options = auto_opts.split()
            for option in automatic_options:
                elements = option.split("=")
                if len(elements) == 2:
                    self._add_template_var(
                        elements[0], '"' + elements[1] + '"')

    def process(self, warn_source=True, default_project_name=None):
        """Process STEM options into 'rose suite-run' options.

        Args:
            warn_source:
                If True, then a warning will be displayed if the source is
                changed.
        """
        # Generate options for source trees
        repos = {}
        repos_with_hosts = {}
        if not self.opts.stem_sources:
            self.opts.stem_sources = ['.']
        self.opts.project = []

        for i, url in enumerate(self.opts.stem_sources):
            try:
                project, url, base, rev, mirror = self._ascertain_project(
                    url,
                    warn_source,
                )
            except ProjectNotFoundException as exc:
                # we couldn't determine the project name (e.g. this is not an
                # SVN version controlled source)
                if i == 0 and default_project_name:
                    # this is the first source in the list (i.e. the rose-stem
                    # source) so we'll fall back to a default project name
                    project = default_project_name
                    base = url
                    rev = mirror = ''
                else:
                    raise exc from None

            self.opts.stem_sources[i] = url
            self.opts.project.append(project)

            if i == 0:
                config_tree = load_rose_config(Path(url) / "rose-stem")
                plugin_result = process_config(config_tree)
                # set environment variables
                # export_environment(plugin_result['env'])
                self.env.update(plugin_result['env'])
                template_type = plugin_result['templating_detected']
                self.template_section = id_templating_section(
                    template_type, with_brackets=True)

            # Versions of variables with hostname prepended for working copies
            url_host = self._prepend_localhost(url)
            base_host = self._prepend_localhost(base)

            if project in repos:
                repos[project].append(url)
                repos_with_hosts[project].append(url_host)
            else:
                repos[project] = [url]
                repos_with_hosts[project] = [url_host]
                self._add_template_var('SOURCE_' + project.upper() + '_REV',
                                        '"' + rev + '"')
                self._add_template_var('SOURCE_' + project.upper() + '_BASE',
                                        '"' + base + '"')
                self._add_template_var('HOST_SOURCE_' + project.upper() +
                                        '_BASE', '"' + base_host + '"')
                self._add_template_var('SOURCE_' + project.upper() +
                                        '_MIRROR', '"' + mirror + '"')
            self.reporter(SourceTreeAddedAsBranchEvent(url))

        for project, branches in repos.items():
            var = 'SOURCE_' + project.upper()
            branchstring = RosePopener.list_to_shell_str(branches)
            self._add_template_var(var, '"' + branchstring + '"')
        for project, branches in repos_with_hosts.items():
            var_host = 'HOST_SOURCE_' + project.upper()
            branchstring = RosePopener.list_to_shell_str(branches)
            self._add_template_var(var_host, '"' + branchstring + '"')

        # Generate the variable containing tasks to run
        if self.opts.stem_groups:
            expanded_groups = []
            for i in self.opts.stem_groups:
                expanded_groups.extend(i.split(','))
            self._add_template_var('RUN_NAMES', str(expanded_groups))

        self._parse_auto_opts()

        # Change into the suite directory
        if getattr(self.opts, 'workflow_conf_dir', None):
            self.reporter(SuiteSelectionEvent(self.opts.workflow_conf_dir))
            self._check_suite_version(
                os.path.join(self.opts.workflow_conf_dir, 'rose-suite.conf'))
        else:
            thissuite = self._this_suite()
            self.fs_util.chdir(thissuite)
            self.reporter(SuiteSelectionEvent(thissuite))

        # Create a default name for the suite; allow override by user
        if not self.opts.workflow_name:
            self.opts.workflow_name = self._generate_name()

        return self.opts

    def enact(self, opts):
        """Apply the results of running rose-stem to "opts"."""
        # export environment variables
        export_environment(self.env)

        # set template variables
        if opts.defines is None:
            opts.defines = []
        opts.defines.extend([
            self.template_section + key + '=' + str(val)
            for key, val in self.template_variables.items()
        ])

        # set workflow name
        if not getattr(opts, 'workflow_name', None):
            opts.workflow_name = self.opts.workflow_name


def get_source_opt_from_args(opts, args):
    """Convert sourcedir given as arg or implied by no arg to
    opts.workflow_conf_dir.

    Possible outcomes:
        No args given:
            Install a rose-stem suite from PWD/rose-stem
        Relative path given:
            Install a rose-stem suite from PWD/arg
        Absolute path given:
            Install a rose-stem suite from specified abs path.

    Returns:
        Cylc options with source attribute added.
    """
    if len(args) == 0:
        # sourcedir not given:
        opts.workflow_conf_dir = None
        return opts
    elif os.path.isabs(args[-1]):
        # sourcedir given, and is abspath:
        opts.workflow_conf_dir = args[-1]
    else:
        # sourcedir given and is not abspath
        opts.workflow_conf_dir = str(Path.cwd() / args[-1])

    return opts


def get_rose_stem_opts():
    """Implement rose stem."""
    # use the cylc install option parser
    parser = get_option_parser()

    # TODO: add any rose stem specific CLI args that might exist
    # On inspection of rose/lib/python/rose/opt_parse.py it turns out that
    # opts.group is stored by the --task option.
    rose_stem_options = OptionGroup(parser, 'Rose Stem Specific Options')
    rose_stem_options.add_option(
        "--task", "--group", "-t", "-g",
        help=(
            "Specify a group name to run. Additional groups can be specified"
            " with further `--group` arguments. The suite will then convert"
            " the groups into a series of tasks to run."
        ),
        action="append",
        metavar="PATH/TO/FLOW",
        default=[],
        dest="stem_groups")
    rose_stem_options.add_option(
        "--source", '-s',
        help=(
            "Specify a source tree to include in a rose-stem suite. The first"
            " source tree must be a working copy, as the location of the suite"
            " and fcm-make config files are taken from it. Further source"
            " trees can be added with additional `--source` arguments."
            " The project which is associated with a given source is normally"
            " automatically determined using FCM, however the project can"
            " be specified as '<project-name>=<project-path>'."
            " Defaults to `.` if not specified."
        ),
        action="append",
        metavar="PATH/TO/FLOW",
        default=[],
        dest="stem_sources")

    parser.add_option_group(rose_stem_options)

    parser.usage = __doc__
    opts, args = parser.parse_args(sys.argv[1:])
    # sliced sys.argv to drop 'rose-stem'
    opts = get_source_opt_from_args(opts, args)

    # Verbosity is set using the Cylc options which decrement and increment
    # verbosity as part of the parser, but rose needs opts.quietness too, so
    # hard set it.
    opts.quietness = 0
    return parser, opts


async def rose_stem(parser, opts):
    try:
        # modify the CLI options to add whatever rose stem would like to add
        rose_stem_runner = StemRunner(opts)
        opts = rose_stem_runner.process()
        rose_stem_runner.enact(opts)

        # call cylc install
        await cylc_install(opts, opts.workflow_conf_dir)

    except CylcError as exc:
        if opts.verbosity > 1:
            raise exc
        print(
            EXC_EXIT.format(
                name=exc.__class__.__name__,
                exc=exc
            ),
            file=sys.stderr
        )


def get_groups_and_sources(
    opts: 'Values',
    rose_template_variables: dict,
) -> Tuple[dict, list]:
    """Extract stem tasks/groups and sources from template variables.

    Returns:
        (groups, sources)

    """
    from cylc.flow.templatevars import load_template_vars

    # fetch cylc template variables
    template_vars = load_template_vars(
        getattr(opts, 'templatevars', None),
        getattr(opts, 'templatevars_file', None),
        getattr(opts, 'templatevars_lists', None),
    )

    # merge with rose template variables
    # NOTE: we apply the Rose vars *before* the Cylc ones, otherwise, you would
    #       not be able to override any default tasks/groups defined in the
    #       rose-suite.conf file using the Cylc template variable options
    #       (e.g. -z).
    template_vars = {
        **rose_template_variables,
        **template_vars,
    }

    # validate groups
    if 'RUN_NAMES' in template_vars:
        raise InputError(
            "Don't set RUN_NAMES manually, use 'tasks' or 'groups'"
        )

    if 'tasks' in template_vars and 'groups' in template_vars:
        raise InputError('Both "tasks" and "groups" provided.')

    if 'TASKS' in template_vars or 'GROUPS' in template_vars:
        raise InputError('Use "tasks" or "groups" not "TASKS" or "GROUPS"')

    if (
        # no tasks/groups defined
        not set(template_vars) & {'tasks', 'groups'}
        # default run names defined
        and 'DEFAULT_RUN_NAMES' in template_vars
    ):
        LOG.info('No tasks/groups defined, falling back to DEFAULT_RUN_NAMES')
        template_vars['groups'] = template_vars['DEFAULT_RUN_NAMES']

    # extract groups / sources
    groups = template_vars.get('tasks', template_vars.get('groups', []))
    sources = template_vars.get('sources', [])

    return groups, sources


def add_install_opts(opts):
    """Add implicit cylc-install options.

    The Rose stem plugin can be actioned by several different Cylc commands
    e.g. "cylc graph", "cylc config", etc, not just "cylc install".

    Because the Rose Stem logic is expecting "cylc install" arguments, we must
    add default values in to avoid issues.

    This loops over the "cylc install" option parser and adds any missing
    options.
    """
    for option in get_option_parser().option_list:
        if option.dest and not hasattr(opts, option.dest):
            setattr(opts, option.dest, option.default)


def update_rose_template_vars(rose_template_vars, unparsed_template_vars):
    from cylc.rose.jinja2_parser import Parser
    parser = Parser()
    rose_template_vars.update({
        key: parser.literal_eval(value)
        for key, value in unparsed_template_vars.items()
    })


def rose_stem_plugin(
    srcdir: Path,
    opts: 'Values',
    rose_template_variables: dict,
):
    """Rose Stem plugin for Cylc.

    The "rose_stem" entry point above is a dedicated Cylc command that calls
    "cylc install".

    This is an automatic configuration plugin that runs for any Cylc command.
    """
    if hasattr(opts, '_rose_stem_runner'):
        # This plugin may be called multiple times for the same operation by
        # compound Cylc .commands (e.g. cylc vip)
        # This retrieves the result of an earlier run to avoid re-running the
        # plugin itself.
        opts._rose_stem_runner.enact(opts)
        update_rose_template_vars(
            rose_template_variables,
            opts._rose_stem_runner.template_variables,
        )
        return

    # this is the first run of the plugin
    LOG.warning('Running the experimental rose-stem Cylc plugin.')
    groups, sources = get_groups_and_sources(opts, rose_template_variables)

    # set the implicit RUN_NAMES template variable
    # rose_template_variables['RUN_NAMES'] = groups

    # avoid mutating the original, this prevents issues with "cylc play"
    # (due to issues with sys.argv and command re-invocation)
    _opts = deepcopy(opts)

    # set rose-stem options (should match get_rose_stem_opts)
    _opts.stem_sources = [str(srcdir.parent)] + sources  # TODO
    # _opts.stem_sources = sources  # TODO
    _opts.stem_groups = groups
    _opts.workflow_conf_dir = srcdir
    _opts.quietness = 0

    # set cylc-install options in case we are not being run by "cylc install"
    add_install_opts(_opts)

    # run rose run
    rose_stem_runner = StemRunner(_opts)
    rose_stem_runner.process(warn_source=False, default_project_name='stem')

    # set template vars
    rose_stem_runner.enact(opts)
    update_rose_template_vars(
        rose_template_variables,
        rose_stem_runner.template_variables,
    )

    # cache the runner for future use
    opts._rose_stem_runner = rose_stem_runner


# * [x] SITE varaibe need to be quoted and not quoted for different stages
# * [x] ALL CLI variables need to be set for fcm_make use cases OR we need to use the flow-processed.cylc?
# * [x] Default source name currently hardcoded - git fallback required
