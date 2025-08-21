#!/bin/sh

#   Copyright (c) 2025 MydriaTech AB
#
#   Permission is hereby granted, free of charge, to any person obtaining a copy
#   of this software and associated documentation files (the "Software"), to
#   deal in the Software without restriction, including without limitation the
#   rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#   sell copies of the Software, and to permit persons to whom the Software is
#   furnished to do so, subject to the following conditions:
#
#   The above copyright notice and this permission notice shall be included in
#   all copies or substantial portions of the Software.
#
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#   FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
#   IN THE SOFTWARE.

export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"

srcDirName="$(ls -d "${CARGO_HOME}"/registry/src/index.crates.io-*)"

targetDirName="./licenses"
rm -rf "$targetDirName"
mkdir -p "$targetDirName"

cargo tree --target x86_64-unknown-linux-musl --prefix none --edges no-build --no-dedupe | \
    sort | \
    uniq | \
    grep -v 'fragtale' | \
    sed '/^[[:space:]]*$/d' | \
    sed 's| (proc-macro)$||' | \
    sed 's| v|-|' | \
    while read -r line ; do
        mkdir "$targetDirName/$line"
        cp -r "$srcDirName/$line"/LICENSE* "$targetDirName/$line/"
        cp "$srcDirName/$line"/COPYRIGHT* "$targetDirName/$line/" 2>/dev/null
        cp "$srcDirName/$line"/COPYING* "$targetDirName/$line/" 2>/dev/null
    done

rustUpToolchainDirName="$(ls -d "$RUSTUP_HOME"/toolchains/*)"
rustDocDirName="$rustUpToolchainDirName/share/doc/rust"
rustVersion="$(rustc --version | sed 's|rustc \(.*\) (.*|\1|g')"
mkdir "$targetDirName/rust-$rustVersion"
cp "$rustDocDirName"/LICENSE* "$targetDirName/rust-$rustVersion/"
cp "$rustDocDirName"/COPYRIGHT* "$targetDirName/rust-$rustVersion/"

echo "Handling special cases..."
depMuslLibcDirName="licenses/musl-libc"
mkdir "$depMuslLibcDirName"
curl --silent \
    -L https://git.musl-libc.org/cgit/musl/plain/COPYRIGHT \
    -o "$depMuslLibcDirName/LICENSE"

cp LICENSE* licenses/
cp COPYRIGHT* licenses/ 2>/dev/null

du -hs licenses/
