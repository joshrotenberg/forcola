# Changelog

## [0.11.1](https://github.com/joshrotenberg/forcola/compare/v0.11.0...v0.11.1) (2026-11-07)


### Bug Fixes

* follow-ups from the descendant tracking review (closes [#122](https://github.com/joshrotenberg/forcola/issues/122)) ([#134](https://github.com/joshrotenberg/forcola/issues/134)) ([58a06c8](https://github.com/joshrotenberg/forcola/commit/58a06c8be27ff3a8b4d7f62fccdb0fcb76ab6590))

## [0.11.0](https://github.com/joshrotenberg/forcola/compare/v0.10.0...v0.11.0) (2026-11-04)


### Features

* check and stop recorded process groups from any BEAM ([#131](https://github.com/joshrotenberg/forcola/issues/131)) ([758e5ed](https://github.com/joshrotenberg/forcola/commit/758e5edc2c7ae8967efdb93633f04069d11d75e5)), closes [#124](https://github.com/joshrotenberg/forcola/issues/124)

## [0.10.0](https://github.com/joshrotenberg/forcola/compare/v0.9.0...v0.10.0) (2026-11-04)


### Features

* add whole-file fcntl lock mode to the shim and Forcola.Lock ([#128](https://github.com/joshrotenberg/forcola/issues/128)) ([4ce0138](https://github.com/joshrotenberg/forcola/commit/4ce0138b26a662495693c4f989db63c963ad1cd6))

## [0.9.0](https://github.com/joshrotenberg/forcola/compare/v0.8.0...v0.9.0) (2026-11-01)


### ⚠ BREAKING CHANGES

* env values must be binaries, nil or false. Other values, such as atoms, now raise ArgumentError before spawning instead of being encoded as strings.

### Features

* support env unsets and interrupt cleanup in every mode ([#108](https://github.com/joshrotenberg/forcola/issues/108)) ([0141f10](https://github.com/joshrotenberg/forcola/commit/0141f102475f5ea5969c16371c40213183d73b54))
* track descendant processes through cleanup ([#110](https://github.com/joshrotenberg/forcola/issues/110)) ([4e9c142](https://github.com/joshrotenberg/forcola/commit/4e9c142d8653c982237202ce3e5ee9d77a1ef083))


### Bug Fixes

* **daemon:** keep the readiness check on schedule while the child floods output (closes [#116](https://github.com/joshrotenberg/forcola/issues/116)) ([#120](https://github.com/joshrotenberg/forcola/issues/120)) ([36ae35d](https://github.com/joshrotenberg/forcola/commit/36ae35d29e5ce29c3d912b4806c442bffbee55d1))
* **duplex:** reject non-iodata in send_line/2 instead of crashing the session (closes [#115](https://github.com/joshrotenberg/forcola/issues/115)) ([#118](https://github.com/joshrotenberg/forcola/issues/118)) ([debd045](https://github.com/joshrotenberg/forcola/commit/debd0457db0ff1df95d2a3cf91087febf112734e))
* include the cgroup drain in the Elixir cleanup budget (closes [#113](https://github.com/joshrotenberg/forcola/issues/113)) ([#121](https://github.com/joshrotenberg/forcola/issues/121)) ([11fc31d](https://github.com/joshrotenberg/forcola/commit/11fc31d2d288e09705412a6f8f98880f738b70b7))
* make EXIT decoders total so a malformed frame never crashes a mode (closes [#112](https://github.com/joshrotenberg/forcola/issues/112)) ([#117](https://github.com/joshrotenberg/forcola/issues/117)) ([a16d773](https://github.com/joshrotenberg/forcola/commit/a16d773b3d5b559e4a2e3d56d297c033b2350de9))
* **shim:** write child stdin from a dedicated thread so a stuck child cannot wedge cleanup (closes [#111](https://github.com/joshrotenberg/forcola/issues/111)) ([#123](https://github.com/joshrotenberg/forcola/issues/123)) ([49576c9](https://github.com/joshrotenberg/forcola/commit/49576c973749880e61d18693dfa50e2ef0fd3a7c))
* validate timeout_ms on every run/2 and Stream.lines/2 call (closes [#114](https://github.com/joshrotenberg/forcola/issues/114)) ([#119](https://github.com/joshrotenberg/forcola/issues/119)) ([da6170c](https://github.com/joshrotenberg/forcola/commit/da6170cf5e27f8a50e1172a3496769f88785adb8))

## [0.8.0](https://github.com/joshrotenberg/forcola/compare/v0.7.0...v0.8.0) (2026-11-01)


### Features

* **shim:** expose negotiated child process identity ([#105](https://github.com/joshrotenberg/forcola/issues/105)) ([48da0c6](https://github.com/joshrotenberg/forcola/commit/48da0c60aad23e8e1c727c0950bfbfb5d03d83b7))


### Bug Fixes

* distinguish spawn failures by stage ([#107](https://github.com/joshrotenberg/forcola/issues/107)) ([b05a9e3](https://github.com/joshrotenberg/forcola/commit/b05a9e3ae3c26b67a1ea8ec812c34d4b0c526b1f))

## [0.7.0](https://github.com/joshrotenberg/forcola/compare/v0.6.0...v0.7.0) (2026-10-23)


### Features

* bound one-shot execution and preserve terminal evidence ([b6de760](https://github.com/joshrotenberg/forcola/commit/b6de760b6cdc3f9e316cff5754a603378930b1ee))

## [0.6.0](https://github.com/joshrotenberg/forcola/compare/v0.5.0...v0.6.0) (2026-10-04)


### Features

* observe raw output during bounded runs (closes [#90](https://github.com/joshrotenberg/forcola/issues/90)) ([#91](https://github.com/joshrotenberg/forcola/issues/91)) ([3d2c55f](https://github.com/joshrotenberg/forcola/commit/3d2c55f60a50dbd5ae862f6678a0ff83fd8f1d45))

## [0.5.0](https://github.com/joshrotenberg/forcola/compare/v0.4.0...v0.5.0) (2026-10-01)


### Features

* bound Stream stderr and partial lines ([#88](https://github.com/joshrotenberg/forcola/issues/88)) ([ca15eca](https://github.com/joshrotenberg/forcola/commit/ca15eca6d41230681071e8c008371c2ea2d9bdc5))
* require cgroup containment before exec ([#87](https://github.com/joshrotenberg/forcola/issues/87)) ([3890bd1](https://github.com/joshrotenberg/forcola/commit/3890bd10c237c3ae78b18cf6a3681f46e144a8a5))

## [0.4.0](https://github.com/joshrotenberg/forcola/compare/v0.3.5...v0.4.0) (2026-10-01)


### Features

* **duplex:** add bounded pull delivery ([7d62c09](https://github.com/joshrotenberg/forcola/commit/7d62c0967586d6e28418c4a67cf60f2db40769ad))
* **duplex:** expose terminal evidence after shutdown ([b96bec6](https://github.com/joshrotenberg/forcola/commit/b96bec698cc63428b50e30b431cd1ea65dbc6f52))

## [0.3.5](https://github.com/joshrotenberg/forcola/compare/v0.3.4...v0.3.5) (2026-09-30)


### Bug Fixes

* support trusted shim paths for escripts ([#76](https://github.com/joshrotenberg/forcola/issues/76)) ([51e1561](https://github.com/joshrotenberg/forcola/commit/51e1561ca6ef66a9ea588e98bee42397f040e87c))

## [0.3.4](https://github.com/joshrotenberg/forcola/compare/v0.3.3...v0.3.4) (2026-09-27)


### Bug Fixes

* close child stdin in run/2 and Stream.lines/2 ([#67](https://github.com/joshrotenberg/forcola/issues/67)) ([f373f84](https://github.com/joshrotenberg/forcola/commit/f373f844507a3a20452259df86800e77a397e8d4))

## [0.3.3](https://github.com/joshrotenberg/forcola/compare/v0.3.2...v0.3.3) (2026-07-31)


### Bug Fixes

* honor explicit supplementary-group clearing ([#60](https://github.com/joshrotenberg/forcola/issues/60)) ([f1543fe](https://github.com/joshrotenberg/forcola/commit/f1543feb597833b95fb2f78685a66f8a1c180e53))
* make shim installs content-aware and atomic ([#59](https://github.com/joshrotenberg/forcola/issues/59)) ([df1e23b](https://github.com/joshrotenberg/forcola/commit/df1e23b43bde2a3c833bcb11410b61e8c3531d8b))
* preserve confirmed Stream idle-timeout status ([#61](https://github.com/joshrotenberg/forcola/issues/61)) ([bfd392c](https://github.com/joshrotenberg/forcola/commit/bfd392ce22c0679203396addadcfd4a45e3e5be9))
* reap residual process-group members after leader exit ([#56](https://github.com/joshrotenberg/forcola/issues/56)) ([0b94c50](https://github.com/joshrotenberg/forcola/commit/0b94c50bd3a60555d4690d3efeda523008038c44))
* surface unconfirmed process teardown ([#58](https://github.com/joshrotenberg/forcola/issues/58)) ([f1e92a1](https://github.com/joshrotenberg/forcola/commit/f1e92a199fd6cb96f662af4025e682eb8f1e257b))

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
