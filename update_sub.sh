#!/bin/bash

git pull --recurse-submodules
git submodule foreach git pull origin master
