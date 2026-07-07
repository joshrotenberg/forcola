# Changelog

## [0.3.2](https://github.com/joshrotenberg/forcola/compare/v0.3.1...v0.3.2) (2026-07-07)


### Bug Fixes

* sync downloaded shim into the build priv so Shim.path/0 resolves on fresh install (closes [#47](https://github.com/joshrotenberg/forcola/issues/47)) ([#48](https://github.com/joshrotenberg/forcola/issues/48)) ([93b8f24](https://github.com/joshrotenberg/forcola/commit/93b8f24d18a8a0f313d51737663712386d6d0c95))

## [0.3.1](https://github.com/joshrotenberg/forcola/compare/v0.3.0...v0.3.1) (2026-07-07)


### Bug Fixes

* point README guide links at processed hexdocs pages ([#45](https://github.com/joshrotenberg/forcola/issues/45)) ([29a70ed](https://github.com/joshrotenberg/forcola/commit/29a70ede9c26a7dbb3d927cbc49d4a42dba1d4b5))

## [0.3.0](https://github.com/joshrotenberg/forcola/compare/v0.2.0...v0.3.0) (2026-07-07)


### Features

* demand-driven backpressure mode for Forcola.Stream.lines/2 (closes [#32](https://github.com/joshrotenberg/forcola/issues/32)) ([#43](https://github.com/joshrotenberg/forcola/issues/43)) ([4a2b7b2](https://github.com/joshrotenberg/forcola/commit/4a2b7b22b36371012869ef3508a04485cecd65d6))
* optional Linux cgroup v2 containment layer (closes [#15](https://github.com/joshrotenberg/forcola/issues/15)) ([#41](https://github.com/joshrotenberg/forcola/issues/41)) ([162c408](https://github.com/joshrotenberg/forcola/commit/162c4085e0ebc2d27ac29d83f84307619aedc081))

## [0.2.0](https://github.com/joshrotenberg/forcola/compare/v0.1.0...v0.2.0) (2026-07-04)


### Features

* idle timeout for Forcola.Stream.lines/2 (closes [#33](https://github.com/joshrotenberg/forcola/issues/33)) ([#37](https://github.com/joshrotenberg/forcola/issues/37)) ([8d84fcd](https://github.com/joshrotenberg/forcola/commit/8d84fcdf5f925b4f2ccfa4720d360c236ccf4480))
* pty support for Forcola.Duplex (closes [#30](https://github.com/joshrotenberg/forcola/issues/30)) ([#39](https://github.com/joshrotenberg/forcola/issues/39)) ([9a284f7](https://github.com/joshrotenberg/forcola/commit/9a284f7f274b8b86c7f862bd4f7bc7648b9d11f6))
* run the child as a different user or group (closes [#31](https://github.com/joshrotenberg/forcola/issues/31)) ([#40](https://github.com/joshrotenberg/forcola/issues/40)) ([9662c6c](https://github.com/joshrotenberg/forcola/commit/9662c6cf62a4ba4b85b24e79c18a86a85888af5e))

## 0.1.0 (2026-07-04)


### Features

* Forcola.Daemon supervised long-running processes (closes [#5](https://github.com/joshrotenberg/forcola/issues/5)) ([#14](https://github.com/joshrotenberg/forcola/issues/14)) ([4768bde](https://github.com/joshrotenberg/forcola/commit/4768bde2fac36c1c3f160718b15c33c09c6e134f))
* Forcola.Duplex bidirectional stdin/stdout sessions (closes [#6](https://github.com/joshrotenberg/forcola/issues/6)) ([#18](https://github.com/joshrotenberg/forcola/issues/18)) ([95a9277](https://github.com/joshrotenberg/forcola/commit/95a92774f5992f9ea71ebf035d4e0dc6a3617c7c))
* Forcola.run/2 bounded one-shot execution (closes [#3](https://github.com/joshrotenberg/forcola/issues/3)) ([#12](https://github.com/joshrotenberg/forcola/issues/12)) ([ebdb25f](https://github.com/joshrotenberg/forcola/commit/ebdb25fa1a493358313a512a041408ccba69decc))
* Forcola.Stream.lines/2 line streaming (closes [#4](https://github.com/joshrotenberg/forcola/issues/4)) ([#13](https://github.com/joshrotenberg/forcola/issues/13)) ([e44c39f](https://github.com/joshrotenberg/forcola/commit/e44c39f6ef9cc25f27e0980e707eb2ff2525c7e0))
* implement forcola_shim (closes [#2](https://github.com/joshrotenberg/forcola/issues/2)) ([#11](https://github.com/joshrotenberg/forcola/issues/11)) ([0fe51fd](https://github.com/joshrotenberg/forcola/commit/0fe51fd86808026cde65c1ce3132a98cfa6c3268))
* scaffold forcola ([#1](https://github.com/joshrotenberg/forcola/issues/1)) ([d9b28e6](https://github.com/joshrotenberg/forcola/commit/d9b28e635bd161a18a7b6f9a7016f67efe3d0092))


### Bug Fixes

* deflake sigterm_ignoring_child_is_sigkilled_after_grace (closes [#19](https://github.com/joshrotenberg/forcola/issues/19)) ([#20](https://github.com/joshrotenberg/forcola/issues/20)) ([9e23ca4](https://github.com/joshrotenberg/forcola/commit/9e23ca4ebe0f148e2f61590e5f4d92166e237b14))
* distinguish ESRCH from EPERM in group_alive (closes [#16](https://github.com/joshrotenberg/forcola/issues/16)) ([#17](https://github.com/joshrotenberg/forcola/issues/17)) ([92b53e4](https://github.com/joshrotenberg/forcola/commit/92b53e4e70dd1c1ae1744d530ed55024adcb669d))
