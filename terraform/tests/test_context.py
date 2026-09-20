"""Exercise Makefile safety checks without contacting GCP."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

TF_ROOT = Path(__file__).resolve().parents[1]


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.log = self.root / 'calls'
        self.env = dict(os.environ, PROJECT='project-a', TF_DATA_DIR=str(self.root),
                        TF_WORKSPACE='default', CALL_LOG=str(self.log))
        self.backend('project-a')
        self.fake_tf = self.root / 'terraform'
        self.fake_tf.write_text('''#!/bin/sh
printf 'terraform %s\\n' "$*" >> "$CALL_LOG"
case "$*" in
  'output -raw region')
    [ -z "$FAIL_REGION" ] || exit 1
    printf 'europe-west1';;
  'output -json scheduler_jobs') printf '["job-one", "job-two"]';;
esac
''')
        self.fake_tf.chmod(0o755)
        gcloud = self.root / 'gcloud'
        gcloud.write_text('''#!/bin/sh
printf 'gcloud %s\\n' "$*" >> "$CALL_LOG"
[ -z "$FAIL_GCLOUD" ]
''')
        gcloud.chmod(0o755)
        self.env['PATH'] = str(self.root) + os.pathsep + self.env['PATH']

    def backend(self, project, prefix='terraform/state'):
        (self.root / 'terraform.tfstate').write_text(json.dumps({
            'backend': {'type': 'gcs', 'config': {
                'bucket': f'{project}-terraform-state', 'prefix': prefix}}}))

    def run_make(self, target, **env):
        return subprocess.run(['make', '--no-print-directory', '-s', '-C', str(TF_ROOT),
                               target, f'TF={self.fake_tf}'],
                              env=dict(self.env, **env), text=True, capture_output=True)

    def test_missing_project_blocks_all_cloud_targets(self):
        for target in ['init', 'bucket-init', 'plan', 'apply', 'destroy', 'state',
                       'output', 'scheduler-pause', 'scheduler-resume']:
            with self.subTest(target=target):
                self.assertNotEqual(self.run_make(target, PROJECT='').returncode, 0)
        self.assertFalse(self.log.exists())

    def test_mismatch_blocks_before_any_terraform_or_gcloud_call(self):
        self.backend('project-b')
        for target in ['plan', 'plan-check', 'apply', 'destroy', 'state', 'output',
                       'scheduler-pause', 'scheduler-resume']:
            with self.subTest(target=target):
                self.assertNotEqual(self.run_make(target).returncode, 0)
        self.assertFalse(self.log.exists())

    def test_uninitialized_wrong_prefix_and_workspace_block(self):
        (self.root / 'terraform.tfstate').unlink()
        self.assertNotEqual(self.run_make('plan').returncode, 0)
        self.backend('project-a', prefix='another-stack')
        self.assertNotEqual(self.run_make('plan').returncode, 0)
        self.backend('project-a')
        self.assertNotEqual(self.run_make('plan', TF_WORKSPACE='other').returncode, 0)
        self.assertFalse(self.log.exists())

    def test_matching_backend_allows_plan_with_explicit_project(self):
        result = self.run_make('plan')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('-var=project_id=project-a', self.log.read_text())

    def test_scheduler_uses_deployed_region_and_requested_action(self):
        for action in ['pause', 'resume']:
            result = self.run_make('scheduler-' + action, REGION='ignored-region')
            self.assertEqual(result.returncode, 0, result.stderr)
            calls = self.log.read_text()
            for job in ['job-one', 'job-two']:
                self.assertIn(f'gcloud scheduler jobs {action} {job} '
                              '--location=europe-west1 --project=project-a', calls)

    def test_output_failure_prevents_gcloud_calls(self):
        self.assertNotEqual(self.run_make('scheduler-resume', FAIL_REGION='1').returncode, 0)
        self.assertNotIn('gcloud', self.log.read_text())

    def test_gcloud_failure_stops_loop(self):
        self.assertNotEqual(self.run_make('scheduler-resume', FAIL_GCLOUD='1').returncode, 0)
        self.assertEqual(self.log.read_text().count('gcloud'), 1)


if __name__ == '__main__':
    unittest.main()
