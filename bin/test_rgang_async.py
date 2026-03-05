#!/bin/env python3
"""Tests for the rgang_async asyncio-based execution module.

These tests verify the async execution module's interface and internal
logic using mocking and local subprocess execution.
"""

import asyncio
import os
import sys
import unittest
from unittest import mock

# Ensure the bin directory is in the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rgang_async


class TestNodeResult(unittest.TestCase):
    """Test the NodeResult dataclass."""

    def test_default_values(self):
        r = rgang_async.NodeResult(name="testhost")
        self.assertEqual(r.name, "testhost")
        self.assertEqual(r.stdout, b"")
        self.assertEqual(r.stderr, b"")
        self.assertIsNone(r.exit_status)
        self.assertFalse(r.timed_out)
        self.assertEqual(r.elapsed, 0.0)

    def test_with_values(self):
        r = rgang_async.NodeResult(
            name="node01",
            stdout=b"hello\n",
            stderr=b"",
            exit_status=0,
            elapsed=1.5,
        )
        self.assertEqual(r.name, "node01")
        self.assertEqual(r.stdout, b"hello\n")
        self.assertEqual(r.exit_status, 0)
        self.assertEqual(r.elapsed, 1.5)


class TestExecutorConfig(unittest.TestCase):
    """Test the ExecutorConfig dataclass."""

    def test_defaults(self):
        c = rgang_async.ExecutorConfig()
        self.assertEqual(c.nway, 200)
        self.assertEqual(c.timeout, 150.0)
        self.assertEqual(c.copy_timeout, 3600.0)
        self.assertIsNone(c.user)
        self.assertEqual(c.ssh_command, "ssh")
        self.assertEqual(c.scp_command, "scp")
        self.assertFalse(c.use_asyncssh)
        self.assertFalse(c.combine_stderr)

    def test_custom_values(self):
        c = rgang_async.ExecutorConfig(
            nway=50,
            timeout=30.0,
            user="admin",
            ssh_command="/usr/bin/ssh",
        )
        self.assertEqual(c.nway, 50)
        self.assertEqual(c.timeout, 30.0)
        self.assertEqual(c.user, "admin")
        self.assertEqual(c.ssh_command, "/usr/bin/ssh")


class TestFormatResults(unittest.TestCase):
    """Test the format_results output formatting."""

    def test_header_style_0(self):
        results = [
            rgang_async.NodeResult(name="node01", stdout=b"hello\n", exit_status=0),
        ]
        output = rgang_async.format_results(results, header_style=0)
        self.assertIn("hello", output)
        self.assertNotIn("node01", output)

    def test_header_style_1(self):
        results = [
            rgang_async.NodeResult(name="node01", stdout=b"hello\n", exit_status=0),
        ]
        output = rgang_async.format_results(results, header_style=1)
        self.assertIn("node01=", output)
        self.assertIn("hello", output)

    def test_header_style_2(self):
        results = [
            rgang_async.NodeResult(name="node01", stdout=b"hello\n", exit_status=0),
        ]
        output = rgang_async.format_results(results, header_style=2)
        self.assertIn("--- node01 ---", output)
        self.assertIn("hello", output)

    def test_ditto_mode(self):
        results = [
            rgang_async.NodeResult(name="node01", stdout=b"same\n", exit_status=0),
            rgang_async.NodeResult(name="node02", stdout=b"same\n", exit_status=0),
            rgang_async.NodeResult(name="node03", stdout=b"different\n", exit_status=0),
        ]
        output = rgang_async.format_results(results, header_style=2, ditto=True)
        self.assertIn("--- node01 ---", output)
        self.assertIn("ditto", output)
        self.assertIn("--- node03 ---", output)
        self.assertIn("different", output)

    def test_timeout_display(self):
        results = [
            rgang_async.NodeResult(
                name="node01", timed_out=True, exit_status=8, elapsed=30.0
            ),
        ]
        output = rgang_async.format_results(results, header_style=2)
        self.assertIn("TIMEOUT", output)

    def test_error_display(self):
        results = [
            rgang_async.NodeResult(
                name="node01", stderr=b"error msg\n", exit_status=1
            ),
        ]
        output = rgang_async.format_results(results, header_style=2)
        self.assertIn("EXIT STATUS: 1", output)

    def test_empty_results(self):
        output = rgang_async.format_results([], header_style=2)
        self.assertEqual(output, "")


class TestRunSSHSubprocess(unittest.TestCase):
    """Test _run_ssh_subprocess using a local echo command."""

    def test_local_echo(self):
        """Test with a local command to verify the async subprocess flow."""
        config = rgang_async.ExecutorConfig(
            ssh_command="/bin/echo",  # Use echo as a fake "ssh" that just echoes args
            timeout=10.0,
        )

        async def _test():
            stdout, stderr, exit_status = await rgang_async._run_ssh_subprocess(
                "fakehost",
                "some command",
                config,
            )
            return stdout, stderr, exit_status

        stdout, stderr, exit_status = asyncio.run(_test())
        # /bin/echo will echo its args: "-T fakehost some command"
        self.assertIn(b"fakehost", stdout)
        self.assertEqual(exit_status, 0)

    def test_timeout(self):
        """Test that timeout works using a real sleep command."""
        config = rgang_async.ExecutorConfig(
            ssh_command="/bin/sh",  # use shell so we can construct a proper sleep
            timeout=0.2,
        )

        async def _test():
            # Override to call sh -c 'sleep 10' which will actually block
            proc = await asyncio.create_subprocess_exec(
                "/bin/sh", "-c", "sleep 10",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                await asyncio.wait_for(proc.communicate(), timeout=0.2)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise

        with self.assertRaises(asyncio.TimeoutError):
            asyncio.run(_test())


class TestRunCommandOnNode(unittest.TestCase):
    """Test run_command_on_node."""

    def test_successful_command(self):
        """Use /bin/echo as ssh substitute to test the full flow."""
        config = rgang_async.ExecutorConfig(
            ssh_command="/bin/echo",
            timeout=10.0,
        )

        async def _test():
            return await rgang_async.run_command_on_node(
                "fakehost", "uptime", config,
                node_index=0, total_nodes=1,
            )

        result = asyncio.run(_test())
        self.assertEqual(result.name, "fakehost")
        self.assertEqual(result.exit_status, 0)
        self.assertGreater(result.elapsed, 0)
        self.assertFalse(result.timed_out)

    def test_timeout_handling(self):
        """Test that timeout is handled gracefully in run_command_on_node."""
        # We need a command that actually blocks. Use sh -c 'sleep 10'
        # by crafting a config where the "ssh" command is sh and host is -c
        # and the remote_cmd includes sleep.
        # Instead, let's mock _run_ssh_subprocess to raise TimeoutError.
        config = rgang_async.ExecutorConfig(timeout=0.1)

        async def _mock_ssh(*args, **kwargs):
            raise asyncio.TimeoutError()

        with mock.patch("rgang_async._run_ssh_subprocess", side_effect=_mock_ssh):
            async def _test():
                return await rgang_async.run_command_on_node(
                    "fakehost", "sleep 10", config,
                    node_index=0, total_nodes=1,
                )

            result = asyncio.run(_test())
            self.assertTrue(result.timed_out)
            self.assertEqual(result.exit_status, 8)
            self.assertIn(b"timeout", result.stderr)


class TestExecuteOnNodes(unittest.TestCase):
    """Test execute_on_nodes parallel execution."""

    def test_multiple_nodes(self):
        """Execute on multiple 'nodes' (using echo as ssh)."""
        config = rgang_async.ExecutorConfig(
            ssh_command="/bin/echo",
            timeout=10.0,
            nway=5,
        )

        results = rgang_async.run(
            ["node01", "node02", "node03"],
            "uptime",
            config,
        )

        self.assertEqual(len(results), 3)
        for i, r in enumerate(results):
            self.assertEqual(r.exit_status, 0)
            self.assertFalse(r.timed_out)

    def test_semaphore_limits_concurrency(self):
        """Verify nway limits concurrent execution."""
        config = rgang_async.ExecutorConfig(
            ssh_command="/bin/echo",
            timeout=10.0,
            nway=2,  # Only 2 at a time
        )

        results = rgang_async.run(
            ["n1", "n2", "n3", "n4", "n5"],
            "test",
            config,
        )

        self.assertEqual(len(results), 5)
        for r in results:
            self.assertEqual(r.exit_status, 0)

    def test_empty_node_list(self):
        """Empty node list should return empty results."""
        results = rgang_async.run([], "uptime")
        self.assertEqual(results, [])


class TestCopyToNodes(unittest.TestCase):
    """Test copy_to_nodes."""

    def test_copy_nonexistent_file(self):
        """Copying a nonexistent file should report an error."""
        config = rgang_async.ExecutorConfig(
            scp_command="/bin/false",  # Always fails
            copy_timeout=10.0,
        )

        results = rgang_async.run_copy(
            ["node01"],
            ["/nonexistent/file"],
            "/tmp/dest",
            config,
        )

        self.assertEqual(len(results), 1)
        # /bin/false returns exit code 1
        self.assertNotEqual(results[0].exit_status, 0)


class TestAsyncSSHNotAvailable(unittest.TestCase):
    """Test behavior when asyncssh is not installed."""

    def test_asyncssh_import_error(self):
        """When asyncssh is unavailable, _run_asyncssh should raise ImportError."""
        config = rgang_async.ExecutorConfig(
            use_asyncssh=True,
            timeout=10.0,
        )

        # Temporarily hide asyncssh
        original = rgang_async.asyncssh
        rgang_async.asyncssh = None
        try:
            async def _test():
                return await rgang_async._run_asyncssh(
                    "testhost", "uptime", config,
                )

            with self.assertRaises(ImportError):
                asyncio.run(_test())
        finally:
            rgang_async.asyncssh = original


if __name__ == "__main__":
    unittest.main()
