# lance-fly

`lance-fly` is a Flight SQL server backed by Lance datasets.

Status: minimal server skeleton.

## Quickstart

Run the server:

```bash
cargo run -- --bind 127.0.0.1:50051
```

Enable logs via `tracing`:

```bash
RUST_LOG=info cargo run -- --bind 127.0.0.1:50051
```

Open a Lance dataset on startup:

```bash
cargo run -- --bind 127.0.0.1:50051 --dataset /path/to/dataset
```

Smoke test:

```bash
cargo test
```

## Contributing

Check out the [CONTRIBUTING.md](./CONTRIBUTING.md) guide for more details on getting started with contributing to this project.

## Getting help

Submit [issues](https://github.com/lance-format/lance-fly/issues/new/choose) for bug report or asking questions in [discussion](https://github.com/lance-format/lance-fly/discussions/new?category=q-a).

#### License

<sup>
Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.
</sup>
