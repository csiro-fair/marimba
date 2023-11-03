
---

<a name="usage"></a>
## Usage

Marimba is based on the [Typer](https://typer.tiangolo.com/) Python package which is self-documenting by default. Try running Marimba to see the default help menu:

```bash
marimba
```

![](docs/img/marimba_default-help.png "marimba_default-help")

The default entry point to start using Marimba is the `new` command. This allows you to create a new Marimba collection, pipeline or collection that adheres to the following standard Marimba structure:

```
{collection}
│
└───distribution                    - 
│
└───pipelines                     - 
│   │
│   └───{pipeline}                - 
│       │
│       └───lib                     - 
│       │   │   pipeline.py       - 
│       │   │   requirement.txt     - 
│       │
│       └───work                    - 
│       │   │
│       │   └───{collection}        - 
│       │
│       │   {pipeline}.log        - 
│       │   pipeline.yml          - 
│       │   metadata.yml            - 
│
└───collection.yml                  - 
└───{collection}.log                - 
```

The usual order you might use the Marimba commands might be:
* `marimba new {collection}`
* `marimba new {pipeline}`
* `marimba new {collection}`
* `marimba qc` - it applicable
* `marimba rename`
* `marimba metadata`
* `marimba convert`
* `marimba distribute`
* ...


<p align="right">(<a href="#readme-top">back to top</a>)</p>
