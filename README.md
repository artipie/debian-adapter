<img src="https://www.artipie.com/logo.svg" width="64px" height="64px"/>

[![EO principles respected here](https://www.elegantobjects.org/badge.svg)](https://www.elegantobjects.org)
[![DevOps By Rultor.com](http://www.rultor.com/b/artipie/debian-adapter)](http://www.rultor.com/p/artipie/debian-adapter)
[![We recommend IntelliJ IDEA](https://www.elegantobjects.org/intellij-idea.svg)](https://www.jetbrains.com/idea/)

[![Javadoc](http://www.javadoc.io/badge/com.artipie/debian-adapter.svg)](http://www.javadoc.io/doc/com.artipie/debian-adapter)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/com.artipie/debian-adapter/blob/master/LICENSE.txt)
[![codecov](https://codecov.io/gh/artipie/debian-adapter/branch/master/graph/badge.svg)](https://codecov.io/gh/artipie/debian-adapter)
[![Hits-of-Code](https://hitsofcode.com/github/artipie/debian-adapter)](https://hitsofcode.com/view/github/artipie/debian-adapter)
[![Maven Central](https://img.shields.io/maven-central/v/com.artipie/debian-adapter.svg)](https://maven-badges.herokuapp.com/maven-central/com.artipie/debian-adapter)
[![PDD status](http://www.0pdd.com/svg?name=artipie/debian-adapter)](http://www.0pdd.com/p?name=artipie/debian-adapter)

This Java library turns your binary storage
(files, S3 objects, anything) into a Debian repository.
You may add it to your binary storage and it will become
a fully-functionable Debian repository, which
[`apt`](https://en.wikipedia.org/wiki/APT_(software)) 
will perfectly understand.

## Debian repository structure

Debian repository [has](https://www.debian.org/doc/manuals/repository-howto/repository-howto#id442666) 
the following structure:
```
(repository root) 
| 
+-dists
  | 
  |-my_repo
  | |-main
  | | |-binary-*
  | | +-source 
  | |-contrib
  | | |-binary-*
  | | +-source 
  | +-non-free
  |   |-binary-*
  |   +-source
  |
  |-testing 
  | ...
  |
  +-unstable 
    | ...
    |
```
`main` contains free package, `non-free` - non-free ones and `contrib` contains free packages which 
depend on non-free ones. `testing` and `unstable` have the same structure as `main` or `testing`.  

`*` in `binary-*` stands for the architecture, currently Debian [supports](https://wiki.debian.org/SupportedArchitectures) 
more than 20 of them. Each `binary-*` directory contains a `Packages.gz` index file. Index files contain 
paths to the individual packages, so packages can be located anywhere in the repository.

File `/dists/my-repo/Release` (or gpg-signed `/dists/my-repo/InRelease` file) [contains](https://wiki.debian.org/DebianRepository/Format#A.22Release.22_files) 
general information about the repository and list of the checksums and paths to the Packages indexes.

## Release files 
Release files contain [information](https://wiki.debian.org/SupportedArchitectures) about distribution 
and checksums for the indexes, they are located in the repository root `/dists/my-repo/Release`. 
Here is the example with minimal fields set:

```text
Codename: artipie #Repository name
Architectures: amd64 #Space separated supported architectures set
Components: main #Repository components set
Date: Sat, 05 Dec 2020 10:35:57 UTC #Creation date
SHA256: #checksums for Packages indexes
  eb8cb7a51d9fe47bde0a32a310b93c01dba531c6f8d14362552f65fcc4277af8 1351 main/binary-amd64/Packages.gz
  c1cfc96b4ca50645c57e10b65fcc89fd1b2b79eb495c9fa035613af7ff97dbff 2564 main/binary-amd64/Packages
```

The most important fields here are `Architectures` (list of the [architectures](https://wiki.debian.org/SupportedArchitectures) 
this repository supports separated by space) and `Components` (a whitespace separated list of 
components or areas, in the layout above components are `main`, `contrib` and `non-free`) as they 
define repository structure. 

`SHA256` field contains list of the repository `Packages` indexes files along with their sha-256 
checksum, size and filename relative to the directory of the `Release` file. Checksums data have 
to be provided for uncompressed files as well even if they are not present in the repository.

## Packages index file

Packages index files are called [Binary Packages Indices](https://wiki.debian.org/DebianRepository/Format#A.22Packages.22_Indices) 
and contains information about packages in paragraphs, each paragraph has the format defined by 
[Debian Policy](https://www.debian.org/doc/debian-policy/#s-binarycontrolfiles) 
and several other fields such as `Filename`, `Size` and checksums, here is an example of one paragraph 
(information about one package):

```text
Package: aglfn
Version: 1.7-3
Architecture: all
Maintainer: Debian Fonts Task Force <pkg-fonts-devel@lists.alioth.debian.org>
Installed-Size: 138
Filename: main/aglfn_1.7-3_all.deb
Size: 29238
MD5sum: cee972bb5e9f9151239e146743a40c9c
SHA1: d404261883ae7bd5a3e35abca99b1256ae070ed5
SHA256: 421a5c6432cb7f9c398aa5c89676884e421ad0fde6bf13d0cadee178bf0daf7c
Section: fonts
Priority: extra
Homepage: http://sourceforge.net/adobe/aglfn/
Description: Adobe Glyph List For New Fonts
 AGL (Adobe Glyph List) maps glyph names to Unicode values ...
```

## Debian sources list

Debian repository can be added to the `apt-get` sources list by editing `/etc/apt/sources.list` file, 
here is the file entry format:

```text
deb [trusted=yes] http://site.example.com/debian distribution component1 component2 component3
```

`distribution` is repository name, `my-repo` in the example above, components are set of the 
repository components `apt` is allowed to work with, parameter `[trusted=yes]` allows to work 
without gpg-signatures.

## How to contribute

Fork repository, make changes, send us a pull request. We will review
your changes and apply them to the `master` branch shortly, provided
they don't violate our quality standards. To avoid frustration, before
sending us your pull request please run full Maven build:

```
$ mvn clean install -Pqulice
```

To avoid build errors use Maven 3.2+.

## Benchmarking

Here is general algorithm to run benchmarks:
 1. Build `rpm-adapter` with `bench` Maven profile enabled: `mvn package -Pbench`
 2. Copy dependencies to `target/` dir: `mvn dependency:copy-dependencies`
 3. Create directory and copy resources required for test into this directory
 4. Run benchmarks with `env BENCH_DIR=/tmp/debian-test java -cp "target/benchmarks.jar:target/classes/*:target/dependency/*" org.openjdk.jmh.Main BenchToRun`, 
 where `/tmp/debian-test` is a directory with resources for tests, `BenchToRun` is benchmark class name.

There are several benchmarks in debian-adapter: `IndexMergeBench` to test indexes merging and other. 

### IndexMergeBench

`IndexMergeBench` calls `MultiPackages.Unique.merge()` to perform Packages indexes merging. To run 
this benchmark it's necessary to provide gziped Packages indexes in the test directory, all the 
files from the directory will be merged. Sample Packages indexes can be found 
[here](https://artipie.s3.amazonaws.com/debian-test/debian-merge.tar.gz).