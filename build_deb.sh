#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Get version from git tag, removing the 'v' prefix
VERSION=$(git describe --tags --abbrev=0 | sed 's/^v//')
if [ -z "$VERSION" ]; then
    echo "Error: No git tag found. Please create a tag (e.g., git tag -a v1.0.0 -m 'Version 1.0.0')."
    exit 1
fi
ARCH=$(dpkg --print-architecture)
PKG=python3-conductor
REL=1
BUILD_DIR=build-deb
STAGE="debpkg/${PKG}_${VERSION}-${REL}_${ARCH}"

cmake -S . -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE=/usr/bin/python3 \
  -DCONDUCTOR_BUILD_EXAMPLES=OFF

cmake --build "$BUILD_DIR" -j"$(nproc)"

rm -rf "$STAGE"
mkdir -p \
  "$STAGE/DEBIAN" \
  "$STAGE/usr/lib/python3/dist-packages" \
  "$STAGE/usr/share/doc/$PKG"

cp "$BUILD_DIR"/conductor*.so "$STAGE/usr/lib/python3/dist-packages/"
cp README.md "$STAGE/usr/share/doc/$PKG/README.md"
cp LICENSE "$STAGE/usr/share/doc/$PKG/copyright"

cat > "$STAGE/DEBIAN/control" <<EOF
Package: $PKG
Version: ${VERSION}-${REL}
Section: python
Priority: optional
Architecture: $ARCH
Maintainer: Fabrizio Pollastri <mxgbot@gmail.com>
Depends: python3 (>= 3.13), libstdc++6
Description: Lightweight periodic task scheduler for Python via pybind11
 conductor provides microsecond periodic scheduling, start-time controls,
 overlap policies, and runtime stats from a C++ backend exposed to Python.
EOF

find "$STAGE" -type f -name '*.so' -exec chmod 0644 {} +
chmod 0755 "$STAGE/DEBIAN"
chmod 0644 "$STAGE/DEBIAN/control"

dpkg-deb --build --root-owner-group "$STAGE"

echo "Built package: ${STAGE}.deb"
