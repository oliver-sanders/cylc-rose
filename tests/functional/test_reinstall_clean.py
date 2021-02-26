# THIS FILE IS PART OF THE ROSE-CYLC PLUGIN FOR THE CYLC SUITE ENGINE.
# Copyright (C) NIWA & British Crown (Met Office) & Contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Functional tests for reinstalling of config files.
This test does the following:

1. Validates a workflow.
2. Installs a workflow with some opts set using -O and
   ROSE_SUITE_OPT_CONF_KEYS.
3. Re-install workflow.
4. After modifying the source ``rose-suite.conf``, re-install the flow again.

At each step it checks the contents of
- ~/cylc-run/temporary-id/rose-suite.conf
- ~/cylc-run/temporary-id/opt/rose-suite-cylc-install.conf
"""

import os
import pytest
import shutil
import subprocess

from pathlib import Path
from uuid import uuid4

from cylc.flow.pathutil import get_workflow_run_dir


@pytest.fixture(scope='module')
def monkeymodule():
    from _pytest.monkeypatch import MonkeyPatch
    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope='module')
def fixture_provide_flow(tmp_path_factory):
    src_flow_name = '11_reinstall_clean'
    workflow_src = Path(__file__).parent / src_flow_name
    test_flow_name = f'cylc-rose-test-{str(uuid4())[:8]}'
    srcpath = (tmp_path_factory.getbasetemp() / test_flow_name)
    flowpath = Path(get_workflow_run_dir(test_flow_name))
    shutil.copytree(workflow_src, srcpath)
    yield {
        'test_flow_name': test_flow_name,
        'flowpath': flowpath,
        'srcpath': srcpath
    }
    shutil.rmtree(srcpath)
    shutil.rmtree(flowpath)


@pytest.fixture(scope='module')
def fixture_install_flow(fixture_provide_flow, monkeymodule):
    result = subprocess.run(
        [
            'cylc', 'install', '-O', 'bar', '-D', '[env]FOO=1',
            '--flow-name', fixture_provide_flow['test_flow_name'],
            '-C', str(fixture_provide_flow['srcpath'])
        ],
        capture_output=True,
        env=os.environ
    )
    yield {
        'fixture_provide_flow': fixture_provide_flow,
        'result': result
    }


def test_cylc_install_run(fixture_install_flow):
    assert fixture_install_flow['result'].returncode == 0


@pytest.mark.parametrize(
    'file_, expect',
    [
        (
            'run1/opt/rose-suite-cylc-install.conf', (
                '# This file records CLI Options.\n\n'
                '!opts=bar\n\n'
                '[env]\n'
                'FOO=1\n'
            )
        ),
    ]
)
def test_cylc_install_files(fixture_install_flow, file_, expect):
    fpath = fixture_install_flow['fixture_provide_flow']['flowpath']
    assert (fpath / file_).read_text() == expect


@pytest.fixture(scope='module')
def fixture_reinstall_flow(fixture_provide_flow, monkeymodule):
    monkeymodule.delenv('ROSE_SUITE_OPT_CONF_KEYS', raising=False)
    result = subprocess.run(
        [
            'cylc', 'reinstall',
            f'{fixture_provide_flow["test_flow_name"]}/run1',
            '-O', 'baz', '-D', '[env]BAR=2'
        ],
        capture_output=True,
        env=os.environ
    )
    yield {
        'fixture_provide_flow': fixture_provide_flow,
        'result': result
    }


def test_cylc_reinstall_run(fixture_reinstall_flow):
    assert fixture_reinstall_flow['result'].returncode == 0


@pytest.mark.parametrize(
    'file_, expect',
    [
        (
            'run1/opt/rose-suite-cylc-install.conf', (
                '# This file records CLI Options.\n\n'
                '!opts=bar baz\n\n'
                '[env]\n'
                'BAR=2\n'
                'FOO=1\n'
            )
        )
    ]
)
def test_cylc_reinstall_files(fixture_reinstall_flow, file_, expect):
    fpath = fixture_reinstall_flow['fixture_provide_flow']['flowpath']
    assert (fpath / file_).read_text() == expect


@pytest.fixture(scope='module')
def fixture_reinstall_flow2(fixture_provide_flow, monkeymodule):
    monkeymodule.delenv('ROSE_SUITE_OPT_CONF_KEYS', raising=False)
    result = subprocess.run(
        [
            'cylc', 'reinstall',
            f'{fixture_provide_flow["test_flow_name"]}/run1',
            '-O', 'baz', '-D', '[env]BAR=2',
            '--clear-rose-install-options'
        ],
        capture_output=True,
    )
    yield {
        'fixture_provide_flow': fixture_provide_flow,
        'result': result
    }


def test_cylc_reinstall_run2(fixture_reinstall_flow2):
    assert fixture_reinstall_flow2['result'].returncode == 0


@pytest.mark.parametrize(
    'file_, expect',
    [
        (
            'run1/opt/rose-suite-cylc-install.conf', (
                '# This file records CLI Options.\n\n'
                '!opts=baz\n\n'
                '[env]\n'
                'BAR=2\n'
            )
        )
    ]
)
def test_cylc_reinstall_files2(fixture_reinstall_flow2, file_, expect):
    fpath = fixture_reinstall_flow2['fixture_provide_flow']['flowpath']
    assert (fpath / file_).read_text() == expect
