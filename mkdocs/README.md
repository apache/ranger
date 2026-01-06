# Apache Ranger Website and Documentation

Welcome to the official website and documentation for Apache Ranger!

## Installation

This documentation site uses [MkDocs](https://www.mkdocs.org/) documentation-focused static site engine, along with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) theme.

Create and activate a new virtual environment:
```bash
python3 -m venv .venv
```

Activate the environment:
```bash
source .venv/bin/activate
```

Install the required dependencies:
```bash
pip install -r requirements.txt
```

Run site locally at http://localhost:8000:
```bash
mkdocs serve --strict
```

To run the site on a different port, such as 8080:
```bash
mkdocs serve --strict --dev-addr=127.0.0.1:8080
```

Build site for production:
```bash
mkdocs build
```

## Contributions

We welcome contributions to the Ranger documentation by our community! See [Contributing](./docs/project/contributing.md) for more information.

To suggest changes, please create a new [issue](https://github.com/apache/ranger/issues) or submit a [pull request](https://github.com/apache/ranger/pulls) with the proposed change!
