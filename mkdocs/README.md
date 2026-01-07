<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

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
