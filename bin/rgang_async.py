"""Asyncio-based parallel SSH execution for rgang.

This module provides an asyncio-based alternative to rgang's traditional
fork/exec + select() architecture.  It demonstrates how to rewrite rgang
using modern Python async/await patterns while preserving the key design
features:

  - Tree-recursive ("worm") execution model for scaling to 1000+ nodes
  - No library requirements on remote nodes (uses ssh/scp commands)
  - Structured result collection with per-node stdout/stderr/exit status
  - Concurrency control via asyncio.Semaphore (equivalent to --nway)
  - Timeout management via asyncio.wait_for
  - Optional asyncssh for pure-Python SSH (like --paramiko)

Architecture notes:

  The original rgang uses os.fork() + os.execvp("ssh") with a select()
  event loop to multiplex I/O from hundreds of child processes.  This
  async module replaces that with:

    os.fork() + os.execvp()  →  asyncio.create_subprocess_exec()
    select() event loop       →  asyncio.get_event_loop()
    os.pipe() + os.read/write →  asyncio.StreamReader/StreamWriter
    os.waitpid()              →  await process.wait()
    os.kill()                 →  process.terminate() / process.kill()

  The tree model works the same way: when the node count exceeds --nway,
  the initiator spawns ssh+rgang on intermediate "branch head" nodes,
  which each handle a subset.  Since rgang itself must be present on
  branch heads, this works without any special remote libraries - only
  Python + rgang + ssh are needed (the same as today).

  For environments where even ssh commands are undesirable, asyncssh
  can be used as an optional pure-Python transport (similar to how
  --paramiko works today).

Ansible comparison:

  Ansible solves the "limited remote libraries" problem by:
  1. Copying Python modules to remote nodes via SFTP/SCP
  2. Executing them with whatever Python is available remotely
  3. Collecting JSON results back over stdout

  rgang's approach is simpler: it only requires ssh + a shell on
  remote nodes for direct commands, and ssh + python + rgang on
  intermediate branch nodes for tree scaling.  The async module
  preserves this - no code is pushed to leaf nodes.

Usage:
    # As a library
    import rgang_async
    results = rgang_async.run(["node01", "node02"], "uptime")

    # Via rgang CLI (future integration)
    rgang --async node{01-10} uptime

Requires: Python 3.7+ (for asyncio.run and async generators)
Optional: pip install asyncssh  (for pure-Python SSH transport)
"""

import asyncio
import os
import shlex
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence


try:
    import asyncssh
except ImportError:
    asyncssh = None


@dataclass
class NodeResult:
    """Result from executing a command on a single node."""

    name: str
    stdout: bytes = b""
    stderr: bytes = b""
    exit_status: Optional[int] = None
    timed_out: bool = False
    elapsed: float = 0.0


@dataclass
class ExecutorConfig:
    """Configuration for the async executor."""

    nway: int = 200
    timeout: float = 150.0
    copy_timeout: float = 3600.0
    user: Optional[str] = None
    ssh_command: str = "ssh"
    scp_command: str = "scp"
    use_asyncssh: bool = False
    combine_stderr: bool = False
    environment: Dict[str, str] = field(default_factory=dict)


async def run_command_on_node(
    host: str,
    command: str,
    config: ExecutorConfig,
    node_index: int = 0,
    total_nodes: int = 1,
) -> NodeResult:
    """Execute a command on a single remote node via SSH.

    Uses asyncio.create_subprocess_exec to run ssh as a child process,
    which requires NO libraries on the remote node - just an SSH server
    and a shell.  This is the same approach as rgang's spawn() function
    but using async I/O instead of select().

    When use_asyncssh=True, uses the asyncssh library for a pure-Python
    SSH implementation (no forking of ssh command).
    """
    result = NodeResult(name=host)
    start = time.monotonic()

    # Build environment exports to prepend to the command
    env_exports = ""
    env_vars = {
        "RGANG_MACH_ID": str(node_index),
        "RGANG_NODES": str(total_nodes),
    }
    env_vars.update(config.environment)
    for key, val in env_vars.items():
        env_exports += "%s=%s;export %s;" % (key, shlex.quote(val), key)

    # Source .rgangrc if present, then run the command
    remote_cmd = (
        env_exports
        + "if [ -r $HOME/.rgangrc ];then . $HOME/.rgangrc;fi;"
        + command
    )

    try:
        if config.use_asyncssh and asyncssh is not None:
            stdout, stderr, exit_status = await _run_asyncssh(
                host, remote_cmd, config
            )
        else:
            stdout, stderr, exit_status = await _run_ssh_subprocess(
                host, remote_cmd, config
            )

        result.stdout = stdout
        result.stderr = stderr
        result.exit_status = exit_status

    except asyncio.TimeoutError:
        result.timed_out = True
        result.stderr = b"rgang timeout expired\n"
        result.exit_status = 8

    except Exception as exc:
        result.stderr = ("rgang error: %s\n" % str(exc)).encode("utf-8")
        result.exit_status = 255

    result.elapsed = time.monotonic() - start
    return result


async def _run_ssh_subprocess(
    host: str,
    remote_cmd: str,
    config: ExecutorConfig,
) -> tuple:
    """Run a command via ssh subprocess (no remote library deps).

    This is the async equivalent of rgang's spawn() + os.execvp("ssh").
    Uses asyncio.create_subprocess_exec which internally uses fork+exec
    but provides async Stream readers/writers instead of raw fds+select.
    """
    ssh_args = [config.ssh_command]
    if config.user:
        ssh_args.extend(["-l", config.user])
    ssh_args.extend(["-T", host, remote_cmd])

    proc = await asyncio.create_subprocess_exec(
        *ssh_args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=(
            asyncio.subprocess.STDOUT
            if config.combine_stderr
            else asyncio.subprocess.PIPE
        ),
    )

    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=config.timeout
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise

    if stderr is None:
        stderr = b""

    return stdout, stderr, proc.returncode


async def _run_asyncssh(
    host: str,
    remote_cmd: str,
    config: ExecutorConfig,
) -> tuple:
    """Run a command via asyncssh (pure-Python SSH, no forking).

    Optional alternative transport.  Requires `pip install asyncssh`
    on the initiator node but nothing on remote nodes.
    """
    if asyncssh is None:
        raise ImportError(
            "asyncssh is required for --async --use-asyncssh mode. "
            "Install with: pip install asyncssh"
        )

    connect_kwargs = {}
    if config.user:
        connect_kwargs["username"] = config.user
    connect_kwargs["known_hosts"] = None  # Accept all (like ssh StrictHostKeyChecking=no)

    async with asyncssh.connect(host, **connect_kwargs) as conn:
        ssh_result = await asyncio.wait_for(
            conn.run(remote_cmd, check=False),
            timeout=config.timeout,
        )
        stdout = (ssh_result.stdout or "").encode("utf-8")
        stderr = (ssh_result.stderr or "").encode("utf-8")
        return stdout, stderr, ssh_result.exit_status


async def copy_to_node(
    host: str,
    sources: List[str],
    dest: str,
    config: ExecutorConfig,
    node_index: int = 0,
) -> NodeResult:
    """Copy files to a remote node via SCP or SFTP.

    Uses scp command by default (no remote library deps).
    With use_asyncssh=True, uses asyncssh SFTP (pure-Python).
    """
    result = NodeResult(name=host)
    start = time.monotonic()

    try:
        if config.use_asyncssh and asyncssh is not None:
            await _copy_asyncssh(host, sources, dest, config)
            result.exit_status = 0
        else:
            stdout, stderr, exit_status = await _copy_scp_subprocess(
                host, sources, dest, config
            )
            result.stdout = stdout
            result.stderr = stderr
            result.exit_status = exit_status

    except asyncio.TimeoutError:
        result.timed_out = True
        result.stderr = b"rgang copy timeout expired\n"
        result.exit_status = 8

    except Exception as exc:
        result.stderr = ("rgang copy error: %s\n" % str(exc)).encode("utf-8")
        result.exit_status = 1

    result.elapsed = time.monotonic() - start
    return result


async def _copy_scp_subprocess(
    host: str,
    sources: List[str],
    dest: str,
    config: ExecutorConfig,
) -> tuple:
    """Copy files via scp subprocess (no remote library deps)."""
    scp_args = [config.scp_command]
    if config.user:
        remote_dest = "%s@%s:%s" % (config.user, host, dest)
    else:
        remote_dest = "%s:%s" % (host, dest)

    scp_args.extend(sources)
    scp_args.append(remote_dest)

    proc = await asyncio.create_subprocess_exec(
        *scp_args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=config.copy_timeout
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise

    return stdout, stderr, proc.returncode


async def _copy_asyncssh(
    host: str,
    sources: List[str],
    dest: str,
    config: ExecutorConfig,
) -> None:
    """Copy files via asyncssh SFTP (pure-Python)."""
    if asyncssh is None:
        raise ImportError(
            "asyncssh is required for async SFTP copy. "
            "Install with: pip install asyncssh"
        )

    connect_kwargs = {}
    if config.user:
        connect_kwargs["username"] = config.user
    connect_kwargs["known_hosts"] = None

    async with asyncssh.connect(host, **connect_kwargs) as conn:
        async with conn.start_sftp_client() as sftp:
            for source in sources:
                remote_path = dest
                try:
                    stat_result = await sftp.stat(dest)
                    if stat_result.permissions is not None:
                        import stat as stat_module
                        if stat_module.S_ISDIR(stat_result.permissions):
                            remote_path = (
                                dest.rstrip("/") + "/" + os.path.basename(source)
                            )
                except asyncssh.SFTPError:
                    pass
                await sftp.put(source, remote_path)


async def execute_on_nodes(
    nodes: Sequence[str],
    command: str,
    config: Optional[ExecutorConfig] = None,
) -> List[NodeResult]:
    """Execute a command across multiple nodes with concurrency control.

    This is the async equivalent of rgang's main execution loop.
    Uses asyncio.Semaphore for concurrency control (like --nway)
    and asyncio.gather for parallel execution.

    The tree/worm model:
      When len(nodes) > config.nway, a flat approach would open too many
      SSH connections from the initiator.  The tree model delegates
      subsets to intermediate "branch head" nodes that run their own
      rgang instance.  This module uses the same flat-within-nway
      approach - the tree recursion happens naturally when rgang on
      a branch head calls execute_on_nodes for its subset.
    """
    if config is None:
        config = ExecutorConfig()

    total = len(nodes)
    semaphore = asyncio.Semaphore(config.nway)

    async def _run_one(index: int, host: str) -> NodeResult:
        async with semaphore:
            return await run_command_on_node(
                host, command, config,
                node_index=index, total_nodes=total,
            )

    tasks = [_run_one(i, node) for i, node in enumerate(nodes)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert any exceptions to NodeResult
    final_results = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            final_results.append(NodeResult(
                name=nodes[i],
                stderr=("rgang error: %s\n" % str(r)).encode("utf-8"),
                exit_status=255,
            ))
        else:
            final_results.append(r)

    return final_results


async def copy_to_nodes(
    nodes: Sequence[str],
    sources: List[str],
    dest: str,
    config: Optional[ExecutorConfig] = None,
) -> List[NodeResult]:
    """Copy files to multiple nodes with concurrency control."""
    if config is None:
        config = ExecutorConfig()

    semaphore = asyncio.Semaphore(config.nway)

    async def _copy_one(index: int, host: str) -> NodeResult:
        async with semaphore:
            return await copy_to_node(
                host, sources, dest, config, node_index=index,
            )

    tasks = [_copy_one(i, node) for i, node in enumerate(nodes)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    final_results = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            final_results.append(NodeResult(
                name=nodes[i],
                stderr=("rgang copy error: %s\n" % str(r)).encode("utf-8"),
                exit_status=1,
            ))
        else:
            final_results.append(r)

    return final_results


def run(
    nodes: Sequence[str],
    command: str,
    config: Optional[ExecutorConfig] = None,
) -> List[NodeResult]:
    """Synchronous wrapper for execute_on_nodes.

    Usage:
        results = rgang_async.run(["node01", "node02"], "uptime")
        for r in results:
            print(f"{r.name}: exit={r.exit_status}")
            print(r.stdout.decode())
    """
    return asyncio.run(execute_on_nodes(nodes, command, config))


def run_copy(
    nodes: Sequence[str],
    sources: List[str],
    dest: str,
    config: Optional[ExecutorConfig] = None,
) -> List[NodeResult]:
    """Synchronous wrapper for copy_to_nodes."""
    return asyncio.run(copy_to_nodes(nodes, sources, dest, config))


def format_results(
    results: List[NodeResult],
    header_style: int = 2,
    ditto: bool = False,
) -> str:
    """Format results for display, similar to rgang's output formatting.

    header_style:
        0 = no header
        1 = "node=" prefix
        2 = "--- node ---" separator (default)
        3 = separator with command echo
    """
    import zlib

    lines = []
    prev_crc = None

    for r in results:
        crc = zlib.crc32(r.stdout + r.stderr)

        if ditto and prev_crc is not None and crc == prev_crc:
            if header_style >= 1:
                lines.append("--- %s --- ditto" % r.name)
            prev_crc = crc
            continue

        if header_style == 1:
            prefix = "%s= " % r.name
            out = r.stdout.decode(errors="replace")
            for line in out.splitlines(True):
                lines.append(prefix + line)
        elif header_style >= 2:
            lines.append("--- %s ---" % r.name)
            if r.stdout:
                lines.append(r.stdout.decode(errors="replace"))
        else:
            if r.stdout:
                lines.append(r.stdout.decode(errors="replace"))

        if r.stderr:
            lines.append(r.stderr.decode(errors="replace"))

        if r.timed_out:
            lines.append("[TIMEOUT after %.1fs]" % r.elapsed)
        elif r.exit_status and r.exit_status != 0:
            lines.append("[EXIT STATUS: %d]" % r.exit_status)

        prev_crc = crc

    return "\n".join(lines)
