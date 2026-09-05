# Development

Use Go 1.24 or newer. Run `make check` and `make test-race` from this repository. No external dependencies, credentials, parent checkout, or workspace are required.

`stream_processor` and `nodes` are the supported public packages. Keep runtime internals under `internal/engine`; consumers own application-specific adapters, authorization, and persistence integration.

Test lifecycle, cancellation, backpressure and checkpoint changes with the race detector. Preserve checkpoint formats during compatible releases and document intentional API or persistence-format changes before tagging a release. New operators need focused behavior tests and public examples.

See [architecture](architecture.md), [usage](how-to-use.md), and the [README](../README.md). Maintainer: PortPowered. Before v1, minor versions may change APIs; consumers should pin immutable release versions.
