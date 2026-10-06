# Changelog

## 0.1.0 (2026-10-06)


### Backend plugin

* accept a user name in Exec ([5545f3e](https://github.com/vicamo/forgejo-lxd-runner/commit/5545f3e1c7b1dbfb5520f8c09bc69fcca2bb0e77))
* add --instance-name-prefix for LXD instance naming ([da97bc9](https://github.com/vicamo/forgejo-lxd-runner/commit/da97bc969383b3783474b4d062c140e9949efc02))
* add --name to run multiple plugin processes side by side ([29d2708](https://github.com/vicamo/forgejo-lxd-runner/commit/29d27083c7ab38e5b28a0473d667b8130791ea6a))
* bring a job's services up and tear them down ([fca07a6](https://github.com/vicamo/forgejo-lxd-runner/commit/fca07a661d40aab3ca60ccb07fb3482583e6c8c0))
* cluster placement via --cluster-target and the cluster-target option ([4f90558](https://github.com/vicamo/forgejo-lxd-runner/commit/4f9055849b88a61e4a5f977847fd0532f0985282))
* exec a job's steps in its container ([65c5b32](https://github.com/vicamo/forgejo-lxd-runner/commit/65c5b3217b6ef304e53e55e339056b6016e61dc4))
* forward cap_add / cap_drop to the job container ([e52c818](https://github.com/vicamo/forgejo-lxd-runner/commit/e52c8189e005b8b8fb11d35ea901766450df1224))
* give every job its own isolated network ([81805b8](https://github.com/vicamo/forgejo-lxd-runner/commit/81805b8696575e2df0edca5f0c6943d77958dea2))
* honour a project backend option ([44c493e](https://github.com/vicamo/forgejo-lxd-runner/commit/44c493e4cb24e3a5bd7c4004450ca1876d06dc45))
* honour a type backend option ([8a3caba](https://github.com/vicamo/forgejo-lxd-runner/commit/8a3caba7553af816daae647b96067ff02654ac12))
* honour an ephemeral backend option ([4d4b9b4](https://github.com/vicamo/forgejo-lxd-runner/commit/4d4b9b4ce41fd35b89bce210b0ede0c070e64188))
* honour environment_timeout with a plugin-side cap ([ca1ca83](https://github.com/vicamo/forgejo-lxd-runner/commit/ca1ca8357353a2263db197c88a5dbcf14c17e7fb))
* honour profiles backend option ([3df2d8b](https://github.com/vicamo/forgejo-lxd-runner/commit/3df2d8b11036fcc758ad9e9175f73ef54a435ce6))
* implement CopyIn ([6035257](https://github.com/vicamo/forgejo-lxd-runner/commit/60352575d4d5bbca6a98aeffacb28280b9350cee))
* implement CopyOut ([d1672ef](https://github.com/vicamo/forgejo-lxd-runner/commit/d1672ef198759cbaf6f646feca088df4da533023))
* implement Create against a local LXD / Incus daemon ([6fee916](https://github.com/vicamo/forgejo-lxd-runner/commit/6fee9168ca6e2f0af55171cb89b814f3660e7180))
* implement Exec ([b70b7b2](https://github.com/vicamo/forgejo-lxd-runner/commit/b70b7b2a5a84c6be65d099bbca1665ed7b6c5f16))
* implement Remove and add end-to-end lifecycle test ([cf08e50](https://github.com/vicamo/forgejo-lxd-runner/commit/cf08e501d9a5671267e2ac61602dfef15ea15cef))
* implement Start ([63e475c](https://github.com/vicamo/forgejo-lxd-runner/commit/63e475c10bfb85b79904bc233a163a2a73dc4328))
* log the package version at startup ([c6f6d82](https://github.com/vicamo/forgejo-lxd-runner/commit/c6f6d8232e237175c81e8faa11e1a5679f6b4426))
* map LXD HTTP errors to gRPC status codes ([3b7218d](https://github.com/vicamo/forgejo-lxd-runner/commit/3b7218db391e3011e67c26f68fd51fada647d2ff))
* populate CreateResponse.arch from the instance ([44a0d5e](https://github.com/vicamo/forgejo-lxd-runner/commit/44a0d5ea9b41c244968d079b6cf7bfe7b3a46c89))
* populate CreateResponse.os from the image metadata ([4f14174](https://github.com/vicamo/forgejo-lxd-runner/commit/4f14174ecb2482236aee71e5cbd1b90649952f8d))
* reflect LXD reachability into grpc.health.v1 status ([43881ef](https://github.com/vicamo/forgejo-lxd-runner/commit/43881ef719466049589bf3247c032ca86fc55d2f))
* remote LXD/Incus over mutual TLS (--endpoint/--client-cert/--client-key) ([96c965f](https://github.com/vicamo/forgejo-lxd-runner/commit/96c965f8fff8d80daf8dc7916ee96409c7f229d4))
* remove a job's container before its instance ([648c8d7](https://github.com/vicamo/forgejo-lxd-runner/commit/648c8d7c228bd7321493a2e8f3e7a5387b9c49ac))
* report POSIX shell semantics in CreateResponse ([604e2cc](https://github.com/vicamo/forgejo-lxd-runner/commit/604e2ccc0f278965f7155220c193eacec4928ab4))
* report the job's environment as image_env ([0cd94d0](https://github.com/vicamo/forgejo-lxd-runner/commit/0cd94d009504bd7d7f0b09eb4e5f131a03d9a4b6))
* run the job in its container ([6a2f67e](https://github.com/vicamo/forgejo-lxd-runner/commit/6a2f67e04674678402e39cea5edd9e499ed9a91f))
* tighten --log-level with choices and case normalisation ([a7a8a3e](https://github.com/vicamo/forgejo-lxd-runner/commit/a7a8a3ef082c54176642abddd8cb9f97b184cb7c))


### Packaging

* add a systemd template unit and deploy docs ([206f8b3](https://github.com/vicamo/forgejo-lxd-runner/commit/206f8b37995b72a4f72ce382675c0170d2dbcae1))
* modernize license metadata for PyPI ([fdb627a](https://github.com/vicamo/forgejo-lxd-runner/commit/fdb627ac446c32a0125c4ecab094ff06cf8bd328))
