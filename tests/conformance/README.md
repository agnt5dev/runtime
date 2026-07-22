# Conformance tests

`v2/fixtures` contains release-published, language-neutral protocol fixtures.
They are validated by the generated Go module today and are consumed by SDK
repositories through the protocol dependency lock.

Later milestones add live SDK-visible workflow behavior and storage semantics
against the community runtime without changing fixture ownership.
