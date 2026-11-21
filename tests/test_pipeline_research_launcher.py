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
