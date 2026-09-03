# LLM Commands (BETA)

<div class="alert alert-info" role="alert">
<strong>BETA:</strong> The LLM commands are currently in beta. Features and output format may change.
</div>

**Export project data to AI-optimized format for use with AI assistants and LLMs.**

The `kbc llm` commands create a "twin format" representation of your Keboola project,
designed for AI assistants to understand and work with your data pipelines.

```
kbc llm [command]
```

## Workflow

1. **Initialize** - Run `kbc llm init` to set up the local directory
2. **Export** - Run `kbc llm export` to generate AI-optimized project data

## Available Commands

|---
| Command | Description
|-|-|-
| [kbc llm init](llm/init.md) | Initialize a new local directory for LLM export. |
| [kbc llm export](llm/export.md) | Export project data to AI-optimized twin format. |

## Next Steps

- [LLM Init](llm/init.md)
- [LLM Export](llm/export.md)
- [All Commands](../commands.md)
