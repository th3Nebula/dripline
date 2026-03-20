# dripline

Query cloud APIs using SQL. One drip at a time.

## Install

```bash
npm install -g dripline
```

## Quick Start

```bash
# Initialize a dripline project
dripline init

# Install a plugin
dripline plugin install aws

# Query away
dripline query "SELECT instance_id, state FROM aws_ec2_instances WHERE region = 'us-east-1'"
```

## Commands

| Command | Description |
|---------|-------------|
| `dripline init` | Create `.dripline/` in current directory |
| `dripline onboard` | Add dripline instructions to CLAUDE.md or AGENTS.md |

## For Agents

Every command supports `--json` for structured output:

```bash
dripline init --json
```

Run `dripline onboard` to add usage instructions to your agent's context file.

## License

MIT
