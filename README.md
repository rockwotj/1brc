# One Billion Row Challenge

An implementation of the [one billion row challenge](https://github.com/gunnarmorling/1brc/) in C++ using [Seastar](https://github.com/scylladb/seastar), [Stringzilla](https://github.com/ashvardanian/StringZilla), and [fast_float](https://github.com/fastfloat/fast_float).

The challenge is supposed to be in Java with no external dependencies, but I sadly don't have time for that kind of fun. Instead I'll make the fastest thing I can think of in the shortest amount of time using C++ and some awesome libraries for direct I/O, SIMD string searching and super fast double parsing.

# Developing

Install dependencies (assuming a recent verison of Ubuntu such as 22.04):

```bash
git submodule update --init --recursive
seastar/install-dependencies.sh
apt-get install -qq ninja-build clang
```

Configure and build:

```
CXX=clang++ CC=clang cmake -Bbuild -S. -GNinja -DCMAKE_BUILD_TYPE=Release
ninja -C build
```

Run the program `./main --input-file ./measurements.txt`:

