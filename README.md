# Fabric Notebook Closed-Loop Development Process

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-Support-orange?style=for-the-badge&logo=buy-me-a-coffee&logoColor=white)](https://buymeacoffee.com/goreavin)

Fabric Notebook Closed-Loop Development Process
An end-to-end process for authoring, deploying, running, monitoring, and correcting PySpark notebooks on Microsoft Fabric — driven entirely from a headless agent environment (SageMaker CodeEditor / devcontainer) with no portal interaction required.

Link to [process document](https://github.com/goreavin/fabric-closed-loop/blob/main/docs/process.md)

## High-Level Closed Loop

```mermaid
---
config:
  theme: base
  fontFamily: "Georgia, serif"
  themeVariables:
    primaryColor: "#B0BEC5"
    primaryTextColor: "#121212"
    primaryBorderColor: "#455A64"
    lineColor: "#455A64"
    secondaryColor: "#ECEFF1"
    secondaryBorderColor: "#455A64"
    tertiaryColor: "#FFFFFF"
    tertiaryBorderColor: "#455A64"
---
flowchart TD
    classDef critical fill:#E3120B,color:#FFFFFF,stroke:#121212,stroke-width:2px

    A["1 · Author .py notebook locally"] --> B["2 · Build Fabric .Notebook format"]
    B --> C["3 · Deploy via fab import"]
    C --> D["4 · Run on HighConcurrency pool"]
    D --> E{"5 · Monitor with nbmon"}
    E -->|Succeeded| F["6 · Query results via SQL endpoint"]
    E -->|Failed| G["Read traceback + Spark Advise"]:::critical
    F --> H{"Results correct?"}
    H -->|Yes| I(["Done"])
    H -->|No| G
    G --> J["7 · Agent edits source .py"]
    J --> B
```

## Support

If you find this project useful, consider buying me a coffee! ☕

<a href="https://buymeacoffee.com/goreavin" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>
