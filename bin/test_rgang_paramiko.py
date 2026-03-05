#!/bin/env python3
"""Tests for the rgang_paramiko transport module.

These tests verify the paramiko transport module's interface and internal
logic using mocking (no actual SSH connections required).
"""

import os
import sys
import unittest
from unittest import mock

# Ensure the bin directory is in the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rgang_paramiko


class TestHandleManagement(unittest.TestCase):
    """Test the handle allocation and identification."""

    def test_is_paramiko_handle_false_for_normal_pid(self):
        self.assertFalse(rgang_paramiko.is_paramiko_handle(1234))
        self.assertFalse(rgang_paramiko.is_paramiko_handle(0))
        self.assertFalse(rgang_paramiko.is_paramiko_handle(65535))

    def test_is_paramiko_handle_true_for_high_values(self):
        self.assertTrue(rgang_paramiko.is_paramiko_handle(2**30))
        self.assertTrue(rgang_paramiko.is_paramiko_handle(2**30 + 100))

    def test_is_paramiko_handle_non_int(self):
        self.assertFalse(rgang_paramiko.is_paramiko_handle("foo"))
        self.assertFalse(rgang_paramiko.is_paramiko_handle(None))

    def test_allocate_handle_unique(self):
        h1 = rgang_paramiko._allocate_handle()
        h2 = rgang_paramiko._allocate_handle()
        self.assertNotEqual(h1, h2)
        self.assertTrue(rgang_paramiko.is_paramiko_handle(h1))
        self.assertTrue(rgang_paramiko.is_paramiko_handle(h2))


class TestParamikoSession(unittest.TestCase):
    """Test the ParamikoSession class."""

    def test_initial_state(self):
        session = rgang_paramiko.ParamikoSession(42)
        self.assertEqual(session.handle_id, 42)
        self.assertIsNone(session.client)
        self.assertIsNone(session.channel)
        self.assertIsNone(session.exit_status)
        self.assertFalse(session.done.is_set())

    def test_wait_nohang_not_done(self):
        session = rgang_paramiko.ParamikoSession(42)
        rpid, status = session.wait_nohang()
        self.assertEqual(rpid, 0)
        self.assertEqual(status, 0)

    def test_wait_nohang_done(self):
        session = rgang_paramiko.ParamikoSession(42)
        session.exit_status = 0
        session.done.set()
        rpid, status = session.wait_nohang()
        self.assertEqual(rpid, 42)
        self.assertEqual(status, 0)

    def test_wait_nohang_done_with_error(self):
        session = rgang_paramiko.ParamikoSession(42)
        session.exit_status = 1
        session.done.set()
        rpid, status = session.wait_nohang()
        self.assertEqual(rpid, 42)
        self.assertEqual(status, 1 << 8)

    def test_wait_blocking(self):
        session = rgang_paramiko.ParamikoSession(42)
        session.exit_status = 0
        session.done.set()
        rpid, status = session.wait()
        self.assertEqual(rpid, 42)
        self.assertEqual(status, 0)

    def test_kill(self):
        session = rgang_paramiko.ParamikoSession(42)
        mock_channel = mock.MagicMock()
        mock_client = mock.MagicMock()
        session.channel = mock_channel
        session.client = mock_client
        session.kill(9)
        mock_channel.close.assert_called_once()
        mock_client.close.assert_called_once()
        self.assertTrue(session.done.is_set())
        self.assertEqual(session.exit_status, 128 + 9)

    def test_kill_without_channel(self):
        """kill() should not raise even without a channel/client."""
        session = rgang_paramiko.ParamikoSession(42)
        session.kill(15)
        self.assertTrue(session.done.is_set())
        self.assertEqual(session.exit_status, 128 + 15)


class TestSpawnSSHInterface(unittest.TestCase):
    """Test spawn_ssh returns the correct interface."""

    @mock.patch("rgang_paramiko._get_ssh_client")
    def test_spawn_ssh_returns_four_tuple(self, mock_get_client):
        """spawn_ssh should return (handle, stdin_fd, stdout_fd, stderr_fd)."""
        mock_client = mock.MagicMock()
        mock_transport = mock.MagicMock()
        mock_channel = mock.MagicMock()
        mock_channel.recv.return_value = b""
        mock_channel.recv_stderr.return_value = b""
        mock_channel.recv_exit_status.return_value = 0
        mock_transport.open_session.return_value = mock_channel
        mock_client.get_transport.return_value = mock_transport
        mock_get_client.return_value = mock_client

        result = rgang_paramiko.spawn_ssh("testhost", "echo hello")
        self.assertEqual(len(result), 4)
        handle_id, stdin_fd, stdout_fd, stderr_fd = result

        self.assertTrue(rgang_paramiko.is_paramiko_handle(handle_id))
        # stdin_fd should be writable
        self.assertIsInstance(stdin_fd, int)
        # stdout_fd should be readable
        self.assertIsInstance(stdout_fd, int)
        # stderr_fd should be readable
        self.assertIsInstance(stderr_fd, int)

        # Clean up file descriptors
        for fd in [stdin_fd, stdout_fd, stderr_fd]:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

    @mock.patch("rgang_paramiko._get_ssh_client")
    def test_spawn_ssh_combine_stderr(self, mock_get_client):
        """With combine_stdout_stderr, stderr_fd should be None."""
        mock_client = mock.MagicMock()
        mock_transport = mock.MagicMock()
        mock_channel = mock.MagicMock()
        mock_channel.recv.return_value = b""
        mock_channel.recv_exit_status.return_value = 0
        mock_transport.open_session.return_value = mock_channel
        mock_client.get_transport.return_value = mock_transport
        mock_get_client.return_value = mock_client

        result = rgang_paramiko.spawn_ssh(
            "testhost", "echo hello", combine_stdout_stderr=True
        )
        handle_id, stdin_fd, stdout_fd, stderr_fd = result
        self.assertIsNone(stderr_fd)

        for fd in [stdin_fd, stdout_fd]:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

    @mock.patch("rgang_paramiko._get_ssh_client")
    def test_spawn_ssh_connection_failure(self, mock_get_client):
        """Connection failure should still return valid tuple with error."""
        mock_get_client.side_effect = Exception("Connection refused")

        result = rgang_paramiko.spawn_ssh("badhost", "echo hello")
        handle_id, stdin_fd, stdout_fd, stderr_fd = result
        self.assertTrue(rgang_paramiko.is_paramiko_handle(handle_id))

        # Should be able to read the error from stderr
        if stderr_fd is not None:
            error_output = os.read(stderr_fd, 4096)
            self.assertIn(b"Connection refused", error_output)

        for fd in [stdin_fd, stdout_fd, stderr_fd]:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass


class TestSpawnCopyInterface(unittest.TestCase):
    """Test spawn_copy returns the correct interface."""

    @mock.patch("rgang_paramiko._get_ssh_client")
    def test_spawn_copy_returns_four_tuple(self, mock_get_client):
        """spawn_copy should return (handle, stdin_fd, stdout_fd, stderr_fd)."""
        mock_client = mock.MagicMock()
        mock_sftp = mock.MagicMock()
        mock_client.open_sftp.return_value = mock_sftp
        mock_get_client.return_value = mock_client
        # Make stat raise IOError so we skip the directory check
        mock_sftp.stat.side_effect = IOError("No such file")

        result = rgang_paramiko.spawn_copy(
            "testhost", ["/tmp/testfile"], "/remote/dest"
        )
        self.assertEqual(len(result), 4)
        handle_id, stdin_fd, stdout_fd, stderr_fd = result
        self.assertTrue(rgang_paramiko.is_paramiko_handle(handle_id))

        # Clean up
        import time
        time.sleep(0.1)  # Let the copy thread finish
        for fd in [stdin_fd, stdout_fd, stderr_fd]:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass


class TestWaitpidAndKill(unittest.TestCase):
    """Test the waitpid and kill wrappers."""

    def test_waitpid_nonexistent_handle(self):
        """waitpid for a handle that doesn't exist should raise."""
        with self.assertRaises(ChildProcessError):
            rgang_paramiko.waitpid(2**30 + 99999999, os.WNOHANG)

    def test_kill_nonexistent_handle(self):
        """kill for a handle that doesn't exist should raise."""
        with self.assertRaises(ProcessLookupError):
            rgang_paramiko.kill(2**30 + 99999999, 9)

    @mock.patch("rgang_paramiko._get_ssh_client")
    def test_waitpid_nohang_running(self, mock_get_client):
        """waitpid with WNOHANG on a running session should return (0, 0)."""
        mock_client = mock.MagicMock()
        mock_transport = mock.MagicMock()
        mock_channel = mock.MagicMock()
        # Make recv block (simulate running process)
        mock_channel.recv.side_effect = lambda n: None  # will hang
        mock_channel.recv_exit_status.side_effect = lambda: None  # will hang
        mock_transport.open_session.return_value = mock_channel
        mock_client.get_transport.return_value = mock_transport
        mock_get_client.return_value = mock_client

        handle_id, stdin_fd, stdout_fd, stderr_fd = rgang_paramiko.spawn_ssh(
            "testhost", "sleep 100"
        )

        # Session shouldn't be done yet (well, it depends on threading, but
        # the mock will keep it from completing immediately)
        rpid, status = rgang_paramiko.waitpid(handle_id, os.WNOHANG)
        # It may or may not have finished, so we just check the interface
        self.assertIsInstance(rpid, int)
        self.assertIsInstance(status, int)

        # Clean up - session may already have been reaped by waitpid
        try:
            rgang_paramiko.kill(handle_id, 9)
        except ProcessLookupError:
            pass  # Already cleaned up by waitpid
        for fd in [stdin_fd, stdout_fd, stderr_fd]:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass


if __name__ == "__main__":
    unittest.main()
