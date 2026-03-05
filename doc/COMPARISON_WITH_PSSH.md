# Feature Comparison: rgang vs pssh

This document compares the features of **rgang** (this project) with
**[pssh](https://github.com/lilydjwg/pssh)** (Parallel SSH), another
popular tool for parallel remote command execution.

Both tools execute commands across multiple hosts in parallel, but they
differ significantly in architecture, scaling approach, and feature sets.

## Architecture

| Aspect | rgang | pssh |
|--------|-------|------|
| Parallelism model | Tree-recursive ("worm") | Flat fork from single node |
| I/O multiplexing | `select()` on raw file descriptors | `select()`/`poll()` with IOMap abstraction |
| Process model | `os.fork()` + `os.execvp()` | `subprocess.Popen()` |
| SSH backend | Configurable (`--rsh`), optional paramiko (`--paramiko`) | Always `ssh` command |
| Scaling | Hierarchical tree, handles 1000+ nodes | Flat parallelism with concurrency limit |
| Deployment | Single Python file | Python package (pip installable) |

## Features in rgang that are missing from pssh

### Tree-recursive execution ("worm" model)

rgang can recursively spawn itself on intermediate branch nodes to create
a hierarchical tree of parallel execution.  With `--nway=N`, rgang spawns
up to N child processes, each of which can further spawn N children on
their respective nodes.  This allows rgang to scale to thousands of nodes
efficiently (e.g., 40,000 nodes in ~3 levels with nway=200) without
overloading a single initiator node.

pssh uses flat parallelism from a single node with a configurable
concurrency limit (`-p`), defaulting to 32 simultaneous connections.

### Node expansion syntax

rgang supports rich inline node expansion:
- Numeric ranges: `node{01-10}` → `node01 node02 ... node10`
- Alphabetic ranges: `node{a-z}` → `nodea nodeb ... nodez`
- Hex/octal: `node{08-0xa}` → `node08 node09 node0a`
- Zero-fill: `node{08-11}` → `node08 node09 node10 node11`
- Combined: `rack{1-3}node{01-10}` → 30 nodes
- Comma lists: `node{a,b,x}` → `nodea nodeb nodex`

pssh reads hosts from files (`-h`) or command-line strings (`-H`) with
no expansion syntax.

### Farmlet directory

rgang can read node lists from named files in a configurable directory
(`--farmlets`), allowing cluster administrators to define named groups
(e.g., `all`, `gpu-nodes`, `row1`).

pssh reads hosts from files passed via `-h` but has no equivalent to a
searchable farmlet directory.

### Ditto mode (`--ditto`)

rgang can detect duplicate output across nodes using CRC32 checksums and
print "ditto" instead of repeating identical output.  This is useful for
large clusters where most nodes produce the same output.

pssh has no equivalent feature.

### Configurable remote shell and copy commands

rgang allows overriding the remote shell (`--rsh`) and copy (`--rcp`)
commands.  This supports non-SSH transports (e.g., `rsh`, custom
wrappers).  The new `--paramiko` option uses python-paramiko directly.

pssh always uses the `ssh` and `scp` commands.

### Serial execution mode (`--serial`)

rgang supports grouped sequential execution where nodes are processed
in batches rather than all at once.  `--serial=100` processes 100 nodes
at a time.

pssh has no equivalent; it processes all tasks with a parallelism limit
but doesn't group them into sequential batches.

### PTY support (`--pty`)

rgang can allocate pseudo-terminals for SSH connections, which is useful
for interactive commands that require a TTY (e.g., password prompts).

pssh uses an SSH_ASKPASS mechanism instead, which works differently.

### Per-node machine index (`RGANG_MACH_ID`)

Each remote command receives a unique sequential index via the
`RGANG_MACH_ID` environment variable.  This can be used for per-node
file names, log directories, or any operation that needs a unique
identifier per host.

pssh sets `PSSH_NODENUM` and `PSSH_NUMNODES`, which is similar but
requires the remote SSH server to accept `SendEnv` for these variables.
rgang sets them via shell commands in the remote execution, avoiding
SSH configuration requirements.

### Additional environment variables

rgang exports several environment variables to remote commands:
- `RGANG_MACH_ID`: Node index
- `RGANG_INITIATOR`: Hostname of the initiating node
- `RGANG_PARENT`: Hostname of the parent rgang process
- `RGANG_PARENT_ID`: Parent's machine index
- `RGANG_NODES`: Total number of nodes

pssh exports `PSSH_NODENUM`, `PSSH_NUMNODES`, and `PSSH_HOST` but relies
on SSH `SendEnv` configuration.

### Remote rc file sourcing (`.rgangrc`)

rgang automatically sources `$HOME/.rgangrc` on each remote node before
executing the user command.  This allows setting up PATH, environment
variables, or aliases on a per-user basis.

pssh has no equivalent feature.

### Error file for retry (`--err-file`)

rgang can write failed/timed-out node names to a file, which can then be
used with `--skip` for retry operations.

pssh writes output to per-host files in a directory (`-o`, `-e`) but has
no dedicated error file for retry workflows.

### Combine stdout/stderr (`--combine`)

rgang can merge stderr into stdout for proper output ordering with
`--combine`.

pssh always keeps stdout and stderr separate.

### Programmatic Python API (`--pyret`, `--pypickle`)

rgang can be imported as a Python module and returns structured results
via pickle serialization.  `--pypickle` produces machine-readable output
suitable for programmatic consumption.

pssh has `psshlib` for programmatic use, but with a different API model
based on a Manager/Task pattern.

### SIGQUIT status reporting

Pressing `Ctrl-\` during rgang execution shows a real-time status summary
including buffer sizes, connection counts, and completion statistics.

pssh has no equivalent interactive status reporting.

### Node skip/exclusion (`--skip`, `-s`)

rgang can exclude specific nodes from execution using `--skip` with full
expansion syntax support, and `-s` to skip the local node.

pssh has `-g` (host glob filter) which filters hosts by pattern, but
cannot exclude specific nodes from an existing list.

### Credential forwarding options

rgang has explicit flags for Kerberos credential forwarding:
- `-f`: Forward non-forwardable credentials
- `-F`: Forward forwardable credentials
- `-N`: Prevent credential forwarding

pssh relies on SSH configuration or `-O` to pass arbitrary SSH options.

## Features in pssh that are missing from rgang

| Feature | Description |
|---------|-------------|
| Per-host output files | `-o outdir` and `-e errdir` save stdout/stderr to per-host files |
| Color output | Colored SUCCESS/FAILURE status messages |
| SSH askpass | Built-in askpass server for password distribution |
| prsync | Parallel rsync for efficient file synchronization |
| pnuke | Parallel remote process killing |
| pslurp | Parallel download (reverse copy from hosts) |
| Inline output modes | `-i` for inline aggregated, `-P` for real-time print |
| Arbitrary SSH options | `-O` to pass any SSH option |
| Host glob filter | `-g` pattern to filter hosts from host files |
| File append mode | `--fileappend` to append to existing output files |
| pip installable | Standard Python package distribution |

## Summary

rgang excels at **large-scale cluster operations** with its tree-recursive
execution model, rich node expansion syntax, and minimal dependencies.
It is designed for environments with thousands of nodes where flat
parallelism becomes a bottleneck.

pssh excels at **ease of use and installation** with its pip-installable
package, clean Python API, and suite of complementary tools (pscp,
prsync, pnuke, pslurp).  It is well-suited for smaller clusters or
environments where a simple parallel SSH tool is sufficient.

The `--paramiko` option in rgang bridges some of the gap by allowing
rgang to use the same SSH library that many modern Python SSH tools use,
while preserving rgang's unique scaling architecture and feature set.

## Modernization: asyncio-based execution (`--async`)

The `--async` option provides a modern Python 3.7+ alternative to rgang's
traditional `os.fork()` + `select()` architecture using `asyncio`:

| Traditional rgang                        | asyncio rgang (`--async`)                 |
|------------------------------------------|-------------------------------------------|
| `os.fork()` + `os.execvp("ssh")` | `asyncio.create_subprocess_exec("ssh")` |
| `select()` event loop | `asyncio.get_event_loop()` |
| `os.pipe()` + `os.read()`/`os.write()` | `asyncio.StreamReader`/`StreamWriter` |
| `os.waitpid()` | `await process.wait()` |
| `os.kill()` | `process.terminate()`/`process.kill()` |
| Manual timeout queue | `asyncio.wait_for(coro, timeout=N)` |
| `nway` via branch counting | `asyncio.Semaphore(nway)` |

### Remote node requirements

Both the traditional and async modes require **nothing extra on remote
nodes** - only an SSH server and a shell.  The `ssh` and `scp` commands
are executed as subprocesses on the **initiator** node.

For pure-Python SSH (no forking), the `asyncssh` library can be used
as an optional transport (similar to `--paramiko`), but this is only
needed on the initiator node.

### How Ansible solves "limited remote libraries"

Ansible takes a different approach to the remote library problem:

1. **Module transfer**: Ansible copies small Python scripts (called
   "Ansible modules") to remote nodes via SFTP/SCP before execution
2. **AnsiballZ**: Modules are compressed and wrapped in a self-extracting
   Python script that bootstraps itself using whatever Python is available
3. **Raw mode**: For nodes without Python, Ansible's `raw` module falls
   back to plain SSH command execution (similar to rgang)
4. **Fact gathering**: Uses `setup` module pushed to each node to collect
   system information

rgang's approach is simpler and more appropriate for its use case:

- **Leaf nodes** (where user commands run): Only need SSH + a shell.
  No Python, no rgang, no libraries pushed.
- **Branch nodes** (intermediate tree nodes for scaling): Need Python +
  rgang, but these are typically the same cluster nodes that already
  have them installed.
- **No code transfer**: Unlike Ansible, rgang never pushes code to
  remote nodes for basic operation.

### Tree scaling with async

The tree/worm model works the same with asyncio. When the node count
exceeds `--nway`, the initiator spawns `ssh <branch-head> rgang ...`
processes, and each branch head runs its own rgang instance to handle
its subset.  The async module uses `asyncio.Semaphore` to limit
concurrent SSH connections, equivalent to the traditional nway branching.

```
# Traditional (fork+exec+select):
rgang node{01-1000} uptime

# Modern async (asyncio subprocess):
rgang --async node{01-1000} uptime

# Pure-Python SSH (no fork, uses asyncssh library):
rgang --async --use-asyncssh node{01-1000} uptime
```
