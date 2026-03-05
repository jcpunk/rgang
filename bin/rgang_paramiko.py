"""Paramiko-based SSH/SCP transport for rgang.

This module provides SSH command execution and SFTP file transfer using
the paramiko library as an alternative to the fork+exec of ssh/scp
commands used by default in rgang.

Pipe-based I/O is used to maintain compatibility with rgang's
select()-based I/O multiplexing.  Each paramiko session returns
(handle_id, stdin_fd, stdout_fd, stderr_fd) matching the interface
of rgang's spawn() function.

Usage:
    rgang --paramiko <nodespec> <command>

Requires: pip install paramiko
"""

import os
import threading

try:
    import paramiko
except ImportError:
    paramiko = None

# Handle registry -- handles start at 2**30 to avoid collisions with real PIDs
_HANDLE_BASE = 2**30
_next_handle = _HANDLE_BASE
_sessions = {}
_lock = threading.Lock()


class ParamikoSession:
    """Manages a paramiko SSH session with pipe-based I/O."""

    def __init__(self, handle_id):
        self.handle_id = handle_id
        self.client = None
        self.channel = None
        self.sftp = None
        self.threads = []
        self.exit_status = None
        self.done = threading.Event()

    def wait_nohang(self):
        """Non-blocking wait. Returns (handle_id, status) or (0, 0)."""
        if self.done.is_set():
            status = (self.exit_status if self.exit_status is not None else 0) << 8
            return self.handle_id, status
        return 0, 0

    def wait(self):
        """Blocking wait. Returns (handle_id, status)."""
        self.done.wait()
        status = (self.exit_status if self.exit_status is not None else 0) << 8
        return self.handle_id, status

    def kill(self, sig):
        """Terminate the session."""
        try:
            if self.channel:
                self.channel.close()
        except Exception:
            pass
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass
        if not self.done.is_set():
            self.exit_status = 128 + sig
            self.done.set()


def is_paramiko_handle(pid):
    """Check if a pid is actually a paramiko session handle."""
    return isinstance(pid, int) and pid >= _HANDLE_BASE


def _allocate_handle():
    """Allocate a new unique session handle."""
    global _next_handle
    with _lock:
        h = _next_handle
        _next_handle += 1
    return h


def _get_ssh_client(hostname, user=None):
    """Create and connect a paramiko SSH client.

    Loads system host keys and SSH config (~/.ssh/config) for host-specific
    settings such as hostname aliases, ports, and identity files.
    """
    if paramiko is None:
        raise ImportError(
            "paramiko is required for --paramiko mode. "
            "Install with: pip install paramiko"
        )
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.load_system_host_keys()
    except Exception:
        pass

    connect_kwargs = {}
    if user:
        connect_kwargs["username"] = user

    # Parse SSH config for host-specific settings
    config_path = os.path.expanduser("~/.ssh/config")
    if os.path.exists(config_path):
        try:
            ssh_config = paramiko.SSHConfig.from_path(config_path)
            host_config = ssh_config.lookup(hostname)
            if "hostname" in host_config:
                hostname = host_config["hostname"]
            if "user" in host_config and not user:
                connect_kwargs["username"] = host_config["user"]
            if "port" in host_config:
                connect_kwargs["port"] = int(host_config["port"])
            if "identityfile" in host_config:
                connect_kwargs["key_filename"] = host_config["identityfile"]
        except Exception:
            pass

    client.connect(hostname, **connect_kwargs)
    return client


def _relay_recv_to_pipe(recv_func, write_fd):
    """Relay data from a paramiko channel recv function to a pipe fd."""
    try:
        while True:
            data = recv_func(65536)
            if not data:
                break
            os.write(write_fd, data)
    except Exception:
        pass
    finally:
        try:
            os.close(write_fd)
        except OSError:
            pass


def _relay_pipe_to_channel(read_fd, channel):
    """Relay data from a pipe fd to a paramiko channel."""
    try:
        while True:
            try:
                data = os.read(read_fd, 65536)
            except OSError:
                break
            if not data:
                break
            channel.sendall(data)
    except Exception:
        pass
    finally:
        try:
            channel.shutdown_write()
        except Exception:
            pass
        try:
            os.close(read_fd)
        except OSError:
            pass


def _monitor_exit(session, channel):
    """Monitor channel for exit status and mark session as done."""
    try:
        session.exit_status = channel.recv_exit_status()
    except Exception:
        if session.exit_status is None:
            session.exit_status = 255
    finally:
        session.done.set()


def spawn_ssh(hostname, command, user=None, combine_stdout_stderr=False):
    """Execute a command on a remote host via paramiko SSH.

    Returns (handle_id, stdin_fd, stdout_fd, stderr_fd) compatible with
    the rgang spawn() interface.  stdin_fd is writable; stdout_fd and
    stderr_fd are readable and work with select().
    """
    if paramiko is None:
        raise ImportError(
            "paramiko is required for --paramiko mode. "
            "Install with: pip install paramiko"
        )

    handle_id = _allocate_handle()
    session = ParamikoSession(handle_id)

    # Create pipe pairs: (read_fd, write_fd)
    stdin_r, stdin_w = os.pipe()
    stdout_r, stdout_w = os.pipe()
    if combine_stdout_stderr:
        stderr_r = None
        stderr_w = None
    else:
        stderr_r, stderr_w = os.pipe()

    try:
        client = _get_ssh_client(hostname, user)
        session.client = client

        channel = client.get_transport().open_session()
        session.channel = channel

        if combine_stdout_stderr:
            channel.set_combine_stderr(True)

        channel.exec_command(command)

        # Start relay threads
        t = threading.Thread(
            target=_relay_recv_to_pipe,
            args=(channel.recv, stdout_w),
            daemon=True,
        )
        t.start()
        session.threads.append(t)

        if not combine_stdout_stderr:
            t = threading.Thread(
                target=_relay_recv_to_pipe,
                args=(channel.recv_stderr, stderr_w),
                daemon=True,
            )
            t.start()
            session.threads.append(t)

        t = threading.Thread(
            target=_relay_pipe_to_channel,
            args=(stdin_r, channel),
            daemon=True,
        )
        t.start()
        session.threads.append(t)

        t = threading.Thread(
            target=_monitor_exit,
            args=(session, channel),
            daemon=True,
        )
        t.start()
        session.threads.append(t)

    except Exception as e:
        # Clean up on connection failure
        for fd in [stdin_r, stdout_w]:
            try:
                os.close(fd)
            except OSError:
                pass
        if stderr_w is not None:
            try:
                os.close(stderr_w)
            except OSError:
                pass
        error_msg = ("paramiko: %s\n" % str(e)).encode("utf-8")
        if not combine_stdout_stderr and stderr_r is not None:
            # Write error to a temporary stderr pipe
            err_r, err_w = os.pipe()
            try:
                os.write(err_w, error_msg)
            except OSError:
                pass
            try:
                os.close(err_w)
            except OSError:
                pass
            try:
                os.close(stderr_r)
            except OSError:
                pass
            stderr_r = err_r
        else:
            # Write error to a temporary stdout pipe
            out_r, out_w = os.pipe()
            try:
                os.write(out_w, error_msg)
            except OSError:
                pass
            try:
                os.close(out_w)
            except OSError:
                pass
            try:
                os.close(stdout_r)
            except OSError:
                pass
            stdout_r = out_r
        session.exit_status = 255
        session.done.set()

    with _lock:
        _sessions[handle_id] = session

    return handle_id, stdin_w, stdout_r, stderr_r


def spawn_copy(hostname, sources, dest, user=None, preserve=False,
               combine_stdout_stderr=False):
    """Copy files to a remote host via paramiko SFTP.

    Returns (handle_id, stdin_fd, stdout_fd, stderr_fd) compatible with
    the rgang spawn() interface.
    """
    if paramiko is None:
        raise ImportError(
            "paramiko is required for --paramiko mode. "
            "Install with: pip install paramiko"
        )

    handle_id = _allocate_handle()
    session = ParamikoSession(handle_id)

    stdin_r, stdin_w = os.pipe()
    stdout_r, stdout_w = os.pipe()
    if combine_stdout_stderr:
        stderr_r = None
        stderr_w = None
    else:
        stderr_r, stderr_w = os.pipe()

    def _do_copy():
        try:
            client = _get_ssh_client(hostname, user)
            session.client = client
            sftp = client.open_sftp()
            session.sftp = sftp

            for source in sources:
                remote_path = dest
                # Check if dest is a directory on remote
                try:
                    import stat as stat_module
                    rstat = sftp.stat(dest)
                    if stat_module.S_ISDIR(rstat.st_mode):
                        remote_path = dest.rstrip("/") + "/" + os.path.basename(source)
                except IOError:
                    pass  # dest doesn't exist yet, use as-is

                sftp.put(source, remote_path)
                if preserve:
                    local_stat = os.stat(source)
                    sftp.chmod(remote_path, local_stat.st_mode & 0o7777)
                    sftp.utime(remote_path, (local_stat.st_atime, local_stat.st_mtime))

            sftp.close()
            client.close()
            session.exit_status = 0
        except Exception as e:
            error_msg = ("paramiko copy: %s\n" % str(e)).encode("utf-8")
            write_fd = stderr_w if not combine_stdout_stderr and stderr_w is not None else stdout_w
            try:
                os.write(write_fd, error_msg)
            except OSError:
                pass
            session.exit_status = 1
        finally:
            for fd in [stdout_w, stdin_r] + ([stderr_w] if stderr_w is not None else []):
                try:
                    os.close(fd)
                except OSError:
                    pass
            session.done.set()

    with _lock:
        _sessions[handle_id] = session

    t = threading.Thread(target=_do_copy, daemon=True)
    t.start()
    session.threads.append(t)

    return handle_id, stdin_w, stdout_r, stderr_r


def waitpid(handle_id, options=0):
    """Wait for a paramiko session. Compatible with os.waitpid() return values.

    Returns (handle_id, status) where status encodes the exit code.
    With WNOHANG, returns (0, 0) if the session hasn't finished yet.
    """
    with _lock:
        session = _sessions.get(handle_id)

    if session is None:
        raise ChildProcessError("No paramiko session with handle %d" % handle_id)

    if options & os.WNOHANG:
        rpid, status = session.wait_nohang()
    else:
        rpid, status = session.wait()

    if rpid != 0:
        with _lock:
            _sessions.pop(handle_id, None)

    return rpid, status


def kill(handle_id, sig):
    """Kill/terminate a paramiko session."""
    with _lock:
        session = _sessions.get(handle_id)

    if session is None:
        raise ProcessLookupError(
            "No paramiko session with handle %d" % handle_id
        )

    session.kill(sig)
