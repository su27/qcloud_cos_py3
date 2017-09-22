#!/usr/bin/env bash

# build the docs
cd docs
make html

# push
cd build/html
git add -A
git commit -m "publishing updated docs"
git push pages gh-pages
