# off-to-mf

## About The Project

This project is provided in compliance with Section 4.6 b. of the [ODbL](https://opendatacommons.org/licenses/odbl/1-0/).

Running this project converts a single Open Food Facts CSV Data Export into a set of JSONL files. Apart from file type conversion, this project also mutates the data structure and drops certain rows and columns as per the needs of the MacroFactor app.

## Getting Started

### Prerequisites

- Python - https://www.python.org/downloads/
- Poetry - https://python-poetry.org/docs/
- Open Food Facts Data - https://world.openfoodfacts.org/data

### Installation

1. Download the latest CSV Data Export from Open Food Facts
2. Create a `data` folder in the project root
3. Move the Open Food Facts Data into the `data` folder
4. Install the project dependencies

```console
poetry install
```
