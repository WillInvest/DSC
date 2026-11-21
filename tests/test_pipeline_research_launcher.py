import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

SOURCE = (Path(__file__).resolve().parents[1] /
          'scripts/pipeline/deployment/node/run-research.py')
SPEC = importlib.util.spec_from_file_location('research_launcher', SOURCE)
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class ResearchLauncherTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        opening = Path(temp.name)
        baseline = opening/'plan.json'
        baseline.write_text(json.dumps(dict(end_block=10, end_hash='0xab')))
        self.boundary = opening/'boundary.json'
        self.boundary.write_text(json.dumps(dict(next_block=11, predecessor_hash='0xab')))
        self.config = {key: key for key in (
            'market_profiles', 'market_verification', 'production_fence', 'revert_ssh_config',
            'hf_history', 'cryo', 'hf_helpers', 'market_drive', 'hf_drive', 'hf_backup_start')}
        self.config.update(market_baseline=str(baseline), hf_opening=str(opening))

    def test_scheduled_pass_uses_committed_market_outputs_before_hf_and_backup(self):
        with patch.object(launcher.subprocess, 'run') as execute:
            launcher.run(self.config)
        calls = [call.args[0] for call in execute.call_args_list]
        self.assertEqual([args[3] for args in calls], [
            'pipeline.main', 'pipeline.research_layout', 'hf_incremental_follow',
            'pipeline.compaction.backup_catalog', 'hf_incremental_archive'])
        self.assertIn('/mnt/storage/dsc-data/metadata/market', calls[0])
        self.assertIn('/mnt/storage/dsc-data/lending/aaveV3/health-factor/windows', calls[2])
        self.assertEqual(calls[2][-2:], ['--local-accepted',
                                       '/mnt/storage/dsc-data/lending/aaveV3/health-factor/local-accepted'])
        self.assertIn('--node-production-fence', calls[0])

    def test_local_pass_finishes_without_drive_configuration(self):
        config = dict(self.config)
        for key in ('market_drive', 'hf_drive', 'hf_backup_start'):
            del config[key]
        with patch.object(launcher.subprocess, 'run') as execute:
            launcher.run(config, local_only=True)
        self.assertEqual([call.args[0][3] for call in execute.call_args_list], [
            'pipeline.main', 'pipeline.research_layout', 'hf_incremental_follow'])

    def test_pinned_hf_runtime_is_available_to_the_local_pass(self):
        config = dict(self.config, hf_input_image='/mnt/storage/dsc-data/example-hf-image',
                      hf_python_overlay='/mnt/storage/dsc-script-releases/example-numpy')
        with patch.object(Path, 'samefile', return_value=True) as samefile:
            with patch.object(launcher.subprocess, 'run') as execute:
                launcher.run(config, local_only=True)
            samefile.assert_called_once_with(config['hf_input_image'])
        self.assertTrue(execute.call_args.kwargs['env']['PYTHONPATH'].endswith(
            config['hf_python_overlay']))

    def test_mismatched_hf_mount_fails_before_writing(self):
        with patch.object(Path, 'samefile', return_value=False):
            with patch.object(launcher.subprocess, 'run') as execute:
                with self.assertRaisesRegex(ValueError, 'Mount the configured HF input image'):
                    launcher.run({'hf_input_image': '/mnt/storage/dsc-data/example-hf-image'})
        execute.assert_not_called()

    def test_mismatched_hf_block_or_hash_fails_before_writing(self):
        for block, block_hash in ((10, '0xab'), (12, '0xab'), (11, '0xcd')):
            with self.subTest(block=block, block_hash=block_hash):
                self.boundary.write_text(json.dumps(dict(next_block=block, predecessor_hash=block_hash)))
                with patch.object(launcher.subprocess, 'run') as execute:
                    with self.assertRaisesRegex(ValueError, 'join the sealed market baseline'):
                        launcher.run(self.config, local_only=True)
                execute.assert_not_called()
