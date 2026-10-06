# Bean Signatures Face the SreRateLimiter / BbrRateLimiter Interfaces

An architecture review found that the AMQP auto-configuration declared the
`hotKeySreRateLimiter` / `hotKeyBbrRateLimiter` beans with their **concrete**
return types (`SreRateLimiterImpl` / `BbrRateLimiterImpl`) and injected them
via concrete-typed `ObjectProvider`s — even though both limiters have
interfaces and every consumer (`DefaultWorkerDecisionHandler`,
`KeyReporterImpl`) uses only interface methods. Combined with the inferred
`@ConditionalOnMissingBean`, a user-supplied custom `SreRateLimiter` or
`BbrRateLimiter` bean did **not** suppress the default: two limiters
coexisted, the concrete-typed provider missed the custom one, and the
documented override contract ("`@ConditionalOnMissingBean` on every bean —
consumers override any component") was silently broken.

We decided to face every bean signature and injection point at the interface:
`@Bean` methods return `SreRateLimiter` / `BbrRateLimiter`, providers and the
consumers' fields take the interface, and `@ConditionalOnMissingBean` is now
explicit (`SreRateLimiter.class` / `BbrRateLimiter.class`). The default wiring
is byte-for-byte identical in every deployment that does not define its own
limiter bean; the only observable change is that the already-documented
override pattern now actually works.

Rejected: keeping concrete `@Bean` return types and changing only the
consumers — mixing concrete producers with interface consumers creates
`NoUniqueBeanDefinitionException` ambiguity when a custom bean exists, which
is worse than the status quo. The interface-typing fix is the minimal change
that makes the assembly layer agree with the project's own Key Patterns.
