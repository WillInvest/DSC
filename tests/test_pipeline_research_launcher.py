import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

SOURCE = (Path(__file__).resolve().parents[1] /
          'scripts/pipeline/deployment/node/run-research.py')
SPEC = importlib.util.spec_from_file_location('research_launcher', SOURCE)
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class ResearchLauncherTests(unittest.TestCase):
    def test_scheduled_pass_uses_committed_market_outputs_before_hf_and_backup(self):
        config = {key: key for key in (
            'market_baseline', 'market_profiles', 'market_verification', 'production_fence',
            'revert_ssh_config', 'hf_opening', 'hf_history', 'cryo', 'hf_helpers',
            'market_drive', 'hf_drive', 'hf_backup_start')}
        with patch.object(launcher.subprocess, 'run') as execute:
            launcher.run(config)
        calls = [call.args[0] for call in execute.call_args_list]
        self.assertEqual([args[3] for args in calls], [
            'pipeline.main', 'pipeline.research_layout', 'hf_incremental_follow',
            'pipeline.compaction.backup_catalog', 'hf_incremental_archive'])
        self.assertIn('/mnt/storage/dsc-data/metadata/market', calls[0])
        self.assertIn('/mnt/storage/dsc-data/lending/aaveV3/health-factor/windows', calls[2])
        self.assertIn('--node-production-fence', calls[0])

    def test_local_pass_finishes_without_drive_configuration(self):
        config = {key: key for key in (
            'market_baseline', 'market_profiles', 'market_verification', 'production_fence',
            'revert_ssh_config', 'hf_opening', 'hf_history', 'cryo', 'hf_helpers')}
        with patch.object(launcher.subprocess, 'run') as execute:
            launcher.run(config, local_only=True)
        self.assertEqual([call.args[0][3] for call in execute.call_args_list], [
            'pipeline.main', 'pipeline.research_layout', 'hf_incremental_follow'])

    def test_pinned_hf_runtime_is_available_to_the_local_pass(self):
        config = {key: key for key in (
            'market_baseline', 'market_profiles', 'market_verification', 'production_fence',
            'revert_ssh_config', 'hf_opening', 'hf_history', 'cryo', 'hf_helpers')}
        config.update(hf_input_image='/mnt/storage/dsc-data/example-hf-image',
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
