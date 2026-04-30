# SupplyChainProjects AI Usecase

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-Support-orange?style=for-the-badge&logo=buy-me-a-coffee&logoColor=white)](https://buymeacoffee.com/goreavin)

This GitLab project hosts work tied to the Supply Chain AI use-case.

## Layout

```
.
└── Fabric/               # Ported Microsoft Fabric workspace for running on AWS SageMaker
    ├── .sagemaker/       # SageMaker bootstrap scripts (unpack, install-deps)
    ├── .tools/claude-home/  # Claude Code config (skills, settings, memories)
    ├── CLAUDE.md         # Claude Code project instructions
    └── ...               # notebooks, drawio diagrams, work artefacts
```

## Quick start on a SageMaker space

```bash
# 1. Clone this repo (the Fabric content lives under ./Fabric)
git clone https://gitlab.com/cslagile/workloads/ai-automation/supplychainprojects/supplychainprojects-ai-usecase.git ~/scp-ai

# 2. Bootstrap Claude Code config (symlinks ~/.claude -> Fabric/.tools/claude-home)
cd ~/scp-ai/Fabric
bash .sagemaker/unpack.sh

# 3. (optional) Install Python deps for tools that need them
bash .sagemaker/install-deps.sh core       # pyarrow + pandas
# bash .sagemaker/install-deps.sh sql      # Fabric Lakehouse SQL client
# bash .sagemaker/install-deps.sh fabric   # Microsoft Fabric CLI
# bash .sagemaker/install-deps.sh mcp      # Model Context Protocol deps
# bash .sagemaker/install-deps.sh all

# 4. First-time Claude login, then start a session
claude login
claude
```

See `Fabric/.sagemaker/README.md` for deeper detail on what the unpack script does and what was intentionally left out of the port.

## Support

If you find this project useful, consider buying me a coffee! ☕

<a href="https://buymeacoffee.com/goreavin" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>
