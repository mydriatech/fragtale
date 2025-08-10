# Fragtale

Turn your planet scale DB into a trustworthy message queue (MQ) with event sourcing capabilities.

## Quick start

See [the quick start guide](doc/quick_start.md) for how to get up and running with Fragtale.

## Motivation, scope and features

Event Sourcing and Command Query Responsibility Segregation (CQRS) are two great software architecture patterns for building resilient and planet-scalable application.
There are many great resources that describes these patterns and their benefits in detail.

Writes to Fragtale are always Events. The Events can be consumed in near real time or queried to get an aggregated view.


### Easy to trust

* Open source and liberal [license](LICENSE-Apache-2.0-with-FWM-Exception-1.0.0).
* Being able to to use Event Sourcing and query the data.
* Consistent planet scale event based system.
* Consistent planet scale querying of historic events for Event Sourcing and Audit.
* [Integrity protection](doc/trusted_events.md) of data at rest to limit required trust in the storage provider.
* Trustworthy record of when events happened for Audit.
* Shared secret are generated and protected by the platform. There are no default secrets.

### Easy to use and scale

* Easy API for connecting micro-service apps including streaming and a synchronous RPC-style REST API abstraction.
* Design that support 1 million events per seconds for each topic (when resources allows it).
* Support extraction of indexed columns for JSON out of the box.
* Event delivery are attempted (roughly) in the order they were received and (at least) once.
* Failed event deliveries should be retried.
* Optionally enforce per topic event document schema validation.
* OAuth2 authentication applied transparently in Kubernetes environments.
* Scales with the database backend and supports Cassandra which is linearly scalable.
* Written events provide an audit trail of what happened without additional write operations.
* Text based protocols and persistence for transparency and portability.

### Easy to operate

* Cloud native and easily deployable using Helm charts.
* Minimal and gitops-friendly configuration.
* Provides [metrics for automatic horizontal scaling](doc/metrics_and_auto_scaling.md).
* Small memory footprint and built using memory safe Rust.
* Support for shared secret roll-over.
* Trivial integration with the [k8ssandra-operator](https://github.com/k8ssandra/k8ssandra-operator).

### Out of scope

* Protect confidentially of sensitive information in event documents at rest.
* Compression of event documents at rest.
* Attestation and Time-Stamping of exported events as a self-contained audit record.
(Since delivered events are guaranteed to have an accurate time and integrity, this could easily be achieved with a third party tool.)


## Name

Event sourcing allows you to see the full story from _fragments_ and tell a _tale_ of what happened while scaling your patterns almost like a _fractale_.

Fragging tails are neither encouraged nor endorsed.

## License

[Apache License 2.0 with Free world makers exception 1.0.0](LICENSE-Apache-2.0-with-FWM-Exception-1.0.0)

The intent of this license to

* Allow makers, innovators, integrators and engineers to do what they do best without blockers.
* Give commercial and non-commercial entities in the free world a competitive advantage.
* Support a long-term sustainable business model where no "open core" or "community edition" is ever needed.

## Governance model

This projects uses the [Benevolent Dictator Governance Model](http://oss-watch.ac.uk/resources/benevolentdictatorgovernancemodel) (site only seem to support plain HTTP).

See also [Code of Conduct](CODE_OF_CONDUCT.md) and [Contributing](CONTRIBUTING.md).
